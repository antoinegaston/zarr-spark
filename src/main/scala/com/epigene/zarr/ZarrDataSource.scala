package com.epigene.zarr

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.lang.ref.SoftReference
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.concurrent.ConcurrentHashMap
import java.util.zip.{GZIPInputStream, InflaterInputStream}

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.github.luben.zstd.ZstdInputStream
import dev.zarr.zarrjava.store.{Store, StoreHandle}
import dev.zarr.zarrjava.v3.{Array => ZarrArray}
import ucar.ma2
import org.apache.hadoop.fs.UnsupportedFileSystemException

/**
 * Spark format: "zarr"
 *
 * Required:
 *   path: Zarr store root (dbfs:/..., s3a://..., abfs://..., gs://..., file:/...)
 *   valuesNode: array node path relative to the store root
 *   columnsNodes: 1D column names array path relative to the store root (comma-separated for multi-index)
 *     (legacy: columnsNode)
 *   indexNodes: 1D index/row names array path relative to the store root (comma-separated for multi-index)
 *     (legacy: indexNode)
 *
 * Layouts:
 *   - Zarr v3: detected by presence of zarr.json at the store root
 *   - Zarr v2: detected by presence of .zarray at the resolved node path (uses zarr-java v2)
 *
 * Values (single node):
 *   valuesNode: "layers/raw" (node path relative to store root)
 *
 * Chunk/partitioning:
 *   useChunkPartitioning: true (default) => partitions align with Zarr chunks
 *   indexPerChunk: override index step if not using chunk partitioning (alias: cellsPerChunk)
 *   columnsPerBlock: block width along columns (default from chunk metadata)
 *
 * Output schema (long format, one row per value):
 *   indexes/columns: one or more string columns (aliases from indexAliases/columnAliases)
 *   value: float
 *
 * Name lookup (required):
 *   columnsNodes: path(s) to 1D column names array (relative to store root)
 *   indexNodes: path(s) to 1D index/row names array (relative to store root)
 *   columnAliases/indexAliases: optional comma-separated aliases; defaults to `columns`/`indexes`.
 *     (legacy: columnAlias/indexAlias)
 *   If a single alias is provided for multiple nodes, suffixes `_1`, `_2`, ... are appended.
 *
 * Name arrays are cached per JVM for reuse across partitions.
 *
 * Only long-format output is supported.
 */
class ZarrDataSource extends TableProvider with DataSourceRegister {
  override def shortName(): String = "zarr"

  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    ZarrSchemas.schemaFor(options)

  override def getTable(schema: StructType, transforms: Array[Transform], properties: java.util.Map[String, String]): Table = {
    val options = new CaseInsensitiveStringMap(properties)
    new ZarrTable(options)
  }
}

object ZarrSchemas {
  def schemaFor(options: CaseInsensitiveStringMap): StructType = {
    ZarrUtils.strReq(options, "valuesNode")
    val (columnsNodes, indexNodes) = ZarrUtils.requireIndexAndColumns(options)
    val (columnAliases, indexAliases) = ZarrUtils.requireAliases(options, columnsNodes, indexNodes)
    val fields = ArrayBuffer[StructField]()
    indexAliases.foreach(alias => fields += StructField(alias, StringType, nullable = false))
    columnAliases.foreach(alias => fields += StructField(alias, StringType, nullable = false))
    fields += StructField("value", FloatType, nullable = false)
    // Build schema via constructor to avoid companion apply binary mismatches across Spark builds
    new StructType(fields.toArray)
  }
}

/** Represents the values array node. */
sealed trait LayerSpec extends Serializable {
  def openArray(options: CaseInsensitiveStringMap, conf: Configuration): (ZarrUtils.ArrayReader, ZarrUtils.ArrayMeta)
}

final case class NodeLayerSpec(nodePath: String) extends LayerSpec {
  override def openArray(options: CaseInsensitiveStringMap, conf: Configuration): (ZarrUtils.ArrayReader, ZarrUtils.ArrayMeta) =
    ZarrUtils.openArray(options, conf, overrideNode = Some(nodePath))
}

/** Pruning constraints derived from pushed filters. */
final case class PruneSpec(
  indexMin: Option[Long] = None,
  indexMax: Option[Long] = None,
  columnBlockStarts: Option[Set[Int]] = None
) extends Serializable {

  def allowsColumnBlockStart(start: Int): Boolean = columnBlockStarts.forall(_.contains(start))

  def overlapsIndex(start: Int, len: Int): Boolean = {
    val end = start.toLong + len.toLong - 1L
    val loOk = indexMax.forall(max => start.toLong <= max)
    val hiOk = indexMin.forall(min => end >= min)
    loOk && hiOk
  }
}

final class ZarrTable(options: CaseInsensitiveStringMap) extends Table with SupportsRead {
  override def name(): String = s"zarr(${options.get("path")})"
  override def schema(): StructType = ZarrSchemas.schemaFor(options)
  override def capabilities(): java.util.Set[TableCapability] =
    Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new ZarrScanBuilder(options)
}

final class ZarrScanBuilder(options: CaseInsensitiveStringMap)
  extends ScanBuilder
    with Batch
    with SupportsPushDownFilters
    with SupportsPushDownRequiredColumns {

  @transient private var pushed: Array[Filter] = Array.empty
  @transient private var filtersForPruning: Array[Filter] = Array.empty
  @transient private var pruneSpec: PruneSpec = PruneSpec()
  @transient private var requiredSchema: StructType = ZarrSchemas.schemaFor(options)

  override def pruneColumns(requiredSchema: StructType): Unit = {
    // Spark tells us which columns are needed downstream
    this.requiredSchema = requiredSchema
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    filtersForPruning = filters
    pushed = Array.empty
    // Return all filters to Spark for correctness; we only use them for partition pruning.
    filters
  }

  override def pushedFilters(): Array[Filter] = pushed

  override def build(): Scan = new Scan {
    override def readSchema(): StructType = requiredSchema
    override def toBatch: Batch = ZarrScanBuilder.this
  }

  override def planInputPartitions(): Array[InputPartition] = {
    val conf = ZarrUtils.hadoopConf(options)
    val optionsMap = options.asCaseSensitiveMap().asScala.toMap

    ZarrUtils.requireIndexAndColumns(options)

    val layerSpec = ZarrUtils.layerSpec(options)

    // Use values node to derive shape/chunks
    val (_, meta0) = layerSpec.openArray(options, conf)
    val shape = meta0.shape
    val (indexSize, columnsSize) = ZarrUtils.axisMapping(shape)

    val useChunk = ZarrUtils.boolOpt(options, "useChunkPartitioning", default = true)
    val (chunkIndex, chunkColumns) = meta0.chunkShape match {
      case Some(ch) if ch.length == 2 => (ch(0), ch(1))
      case _ => (4096, 1024)
    }

    val indexPerChunk = ZarrUtils.intOpt(
      options,
      "indexPerChunk",
      default = ZarrUtils.intOpt(options, "cellsPerChunk", default = chunkIndex)
    )
    val columnsPerBlock = ZarrUtils.intOpt(options, "columnsPerBlock", default = chunkColumns)

    val indexStep = if (useChunk) chunkIndex else indexPerChunk

    // Build pruning constraints now that we know columnsPerBlock.
    pruneSpec = ZarrFilters.toPruneSpec(filtersForPruning, options, conf, columnsPerBlock)

    val parts = for {
      indexStart <- 0 until indexSize by indexStep
      columnStart <- 0 until columnsSize by columnsPerBlock
      indexLen = Math.min(indexStep, indexSize - indexStart)
      columnLen = Math.min(columnsPerBlock, columnsSize - columnStart)
      if pruneSpec.overlapsIndex(indexStart, indexLen)
      if pruneSpec.allowsColumnBlockStart(columnStart)
    } yield {
      ZarrInputPartition(
        options = optionsMap,
        layerSpec = layerSpec,
        indexStart = indexStart,
        indexLen = indexLen,
        columnStart = columnStart,
        columnLen = columnLen,
        requiredFields = requiredSchema.fieldNames
      )
    }

    parts.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory =
    new ZarrPartitionReaderFactory()
}

final case class ZarrInputPartition(
  options: Map[String, String],
  layerSpec: LayerSpec,
  indexStart: Int,
  indexLen: Int,
  columnStart: Int,
  columnLen: Int,
  requiredFields: Array[String]
) extends InputPartition

final class ZarrPartitionReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    new ZarrPartitionReader(partition.asInstanceOf[ZarrInputPartition])
}

final class ZarrPartitionReader(p: ZarrInputPartition) extends PartitionReader[InternalRow] {

  private val optionsCI = new CaseInsensitiveStringMap(p.options.asJava)
  private val conf = ZarrUtils.hadoopConf(optionsCI)
  private val (arr, meta) = p.layerSpec.openArray(optionsCI, conf)

  private val shape = meta.shape
  private val (indexSize, columnsSize) = ZarrUtils.axisMapping(shape)

  private val (offset: Array[Long], readShape: Array[Int]) =
    (Array(p.indexStart.toLong, p.columnStart.toLong), Array(p.indexLen, p.columnLen))

  private val slab: ma2.Array = arr.read(offset, readShape)
  private val it = slab.getIndexIterator

  private val requiredFields = p.requiredFields
  private val requiredFieldSet = requiredFields.toSet
  private val (columnsNodes, indexNodes) = ZarrUtils.requireIndexAndColumns(optionsCI)
  private val (columnAliases, indexAliases) = ZarrUtils.requireAliases(optionsCI, columnsNodes, indexNodes)

  private val columnAliasToNames: Map[String, Array[String]] =
    loadNameArrays(columnsNodes, columnAliases, columnsSize, axisLabel = "columnsNodes")
  private val indexAliasToNames: Map[String, Array[String]] =
    loadNameArrays(indexNodes, indexAliases, indexSize, axisLabel = "indexNodes")

  private var localRow = 0
  private var localColumn = 0

  override def next(): Boolean = localRow < p.indexLen

  override def get(): InternalRow = {
    val value = it.getFloatNext
    val indexValue = p.indexStart.toLong + localRow.toLong
    val columnIndex = p.columnStart + localColumn

    val row = new GenericInternalRow(requiredFields.length)
    var i = 0
    while (i < requiredFields.length) {
      requiredFields(i) match {
        case "value" => row.setFloat(i, value)
        case name =>
          indexAliasToNames.get(name) match {
            case Some(arr) =>
              val v = if (arr == null) null else arr(indexValue.toInt)
              row.update(i, if (v == null) null else UTF8String.fromString(v))
            case None =>
              columnAliasToNames.get(name) match {
                case Some(arr) =>
                  val v = if (arr == null) null else arr(columnIndex)
                  row.update(i, if (v == null) null else UTF8String.fromString(v))
                case None => row.update(i, null)
              }
          }
      }
      i += 1
    }

    localColumn += 1
    if (localColumn >= p.columnLen) {
      localColumn = 0
      localRow += 1
    }
    row
  }

  private def loadNameArrays(
      nodes: Seq[String],
      aliases: Seq[String],
      axisSize: Int,
      axisLabel: String
  ): Map[String, Array[String]] = {
    val builder = Map.newBuilder[String, Array[String]]
    nodes.zip(aliases).foreach { case (node, alias) =>
      if (requiredFieldSet.contains(alias)) {
        val arr = ZarrUtils.cachedStringArray(optionsCI, conf, node)
        if (arr.length < axisSize) {
          throw new IllegalArgumentException(
            s"$axisLabel '$node' length ${arr.length} is less than axis dimension $axisSize."
          )
        }
        builder += alias -> arr
      }
    }
    builder.result()
  }

  override def close(): Unit = ()
}

object ZarrUtils {

  trait ArrayReader {
    def read(offset: Array[Long], shape: Array[Int]): ma2.Array
    def shape: Array[Int]
    def chunkShape: Option[Array[Int]]
  }

  final case class ArrayMeta(shape: Array[Int], chunkShape: Option[Array[Int]])

  def hadoopConf(options: CaseInsensitiveStringMap): Configuration = {
    val base = SparkSession.getActiveSession
      .map(_.sparkContext.hadoopConfiguration)
      .orElse(SparkSession.getDefaultSession.map(_.sparkContext.hadoopConfiguration))
      .getOrElse {
        val env = SparkEnv.get
        if (env != null) {
          val conf = new Configuration()
          env.conf.getAll.foreach {
            case (k, v) if k.startsWith("spark.hadoop.") =>
              conf.set(k.stripPrefix("spark.hadoop."), v)
            case _ => ()
          }
          conf
        } else {
          new Configuration()
        }
      }

    options.asCaseSensitiveMap().asScala.foreach { case (k, v) =>
      if (k.startsWith("hadoop.")) base.set(k.stripPrefix("hadoop."), v)
    }
    base
  }

  def layerSpec(options: CaseInsensitiveStringMap): LayerSpec = {
    val nodePath = strReq(options, "valuesNode")
    if (nodePath.contains(",")) {
      throw new IllegalArgumentException("Option 'valuesNode' expects a single node path.")
    }
    NodeLayerSpec(nodePath)
  }

  /**
   * Returns:
   *  indexSize, columnsSize
   * Normalize to a logical 2D view [index x columns] per values node.
   */
  def axisMapping(shape: Array[Int]): (Int, Int) = {
    if (shape.length != 2) {
      throw new IllegalArgumentException(s"Values node expects 2D arrays, got ${shape.length}D")
    }
    (shape(0), shape(1))
  }

  def requireIndexAndColumns(options: CaseInsensitiveStringMap): (Seq[String], Seq[String]) = {
    val columns = listReqAny(options, Seq("columnsNodes", "columnsNode"))
    val index = listReqAny(options, Seq("indexNodes", "indexNode"))
    (columns, index)
  }

  def requireAliases(
      options: CaseInsensitiveStringMap,
      columnsNodes: Seq[String],
      indexNodes: Seq[String]
  ): (Seq[String], Seq[String]) = {
    val columnAliases = aliasList(options, Seq("columnAliases", "columnAlias"), columnsNodes.size, defaultBase = "columns")
    val indexAliases = aliasList(options, Seq("indexAliases", "indexAlias"), indexNodes.size, defaultBase = "indexes")
    val reserved = Set("value")
    val all = columnAliases ++ indexAliases
    if (all.distinct.size != all.size) {
      throw new IllegalArgumentException("columnAliases and indexAliases values must be unique.")
    }
    if (all.exists(reserved.contains)) {
      throw new IllegalArgumentException("columnAliases and indexAliases cannot be 'value'.")
    }
    (columnAliases, indexAliases)
  }

  def resolveNodePath(node: String): String = {
    val cleanNode = Option(node).getOrElse("").trim
    if (cleanNode.isEmpty) "" else cleanNode.stripPrefix("/").stripSuffix("/")
  }

  private def localPathForRoot(root: String): Option[java.nio.file.Path] = {
    val trimmed = root.trim
    if (trimmed.isEmpty) return None
    try {
      if (trimmed.startsWith("file:")) {
        Some(Paths.get(new java.net.URI(trimmed)))
      } else if (trimmed.startsWith("/")) {
        Some(Paths.get(trimmed))
      } else {
        None
      }
    } catch {
      case _: Exception => None
    }
  }

  def openArray(options: CaseInsensitiveStringMap, conf: Configuration, overrideNode: Option[String]): (ArrayReader, ArrayMeta) = {
    val roots = normalizeStoreRoots(strReq(options, "path"))
    val fullNode = resolveNodePath(overrideNode.getOrElse(""))
    val forceV3 = boolOpt(options, "forceV3", default = false)

    val checkedPaths = scala.collection.mutable.LinkedHashSet[String]()
    var lastErr: Throwable = null

    def recordErr(t: Throwable): Unit = {
      if (lastErr == null) {
        lastErr = t
      } else if (lastErr.isInstanceOf[UnsupportedFileSystemException] &&
          !t.isInstanceOf[UnsupportedFileSystemException]) {
        lastErr = t
      }
    }

    roots.foreach { root =>
      val storeRoot = new Path(root)
      val fsOpt = filesystemForRoot(root, conf)
      if (fsOpt.isEmpty) {
        recordErr(new UnsupportedFileSystemException(s"No FileSystem for scheme '${storeRoot.toUri.getScheme}'"))
      } else {
        val fs = fsOpt.get
        val arrayPath = if (fullNode.nonEmpty) new Path(storeRoot, fullNode) else storeRoot
        val v3RootMeta = new Path(storeRoot, "zarr.json")
        val v3ArrayMeta = new Path(arrayPath, "zarr.json")
        val v2Meta = new Path(arrayPath, ".zarray")

        checkedPaths += v3RootMeta.toString
        checkedPaths += v3ArrayMeta.toString
        checkedPaths += v2Meta.toString

      def isNoOpsAllowed(t: Throwable): Boolean = t match {
        case _: UnsupportedOperationException => true
        case r: java.rmi.RemoteException => Option(r.getCause).exists(isNoOpsAllowed)
        case _ => false
      }

      def isMissingMeta(t: Throwable): Boolean = t match {
        case _: java.io.FileNotFoundException => true
        case _: java.nio.file.NoSuchFileException => true
        case r: java.rmi.RemoteException => Option(r.getCause).exists(isMissingMeta)
        case ioe: java.io.IOException =>
          Option(ioe.getMessage).exists(_.toLowerCase.contains("zarray"))
        case iae: IllegalArgumentException =>
          Option(iae.getMessage).exists(_.toLowerCase.contains("zarray"))
        case _ => false
      }

        val localRoot = localPathForRoot(root)
        val localV3RootMetaExists = localRoot.exists { p =>
          Files.exists(p.resolve("zarr.json"))
        }
        val localV3ArrayMetaExists = localRoot.exists { p =>
          if (fullNode.isEmpty) false else Files.exists(p.resolve(fullNode).resolve("zarr.json"))
        }

        val hasV3RootMeta = localV3RootMetaExists || (try fs.exists(v3RootMeta) catch {
          case t if isNoOpsAllowed(t) => false
        })
        val hasV3ArrayMeta = localV3ArrayMetaExists || (try fs.exists(v3ArrayMeta) catch {
          case t if isNoOpsAllowed(t) => false
        })

      def openV3(): Option[(ArrayReader, ArrayMeta)] = {
        try {
          val store: Store = new HadoopZarrStore(root, conf)
          val storeHandle = new StoreHandle(store)
          val keys = fullNode.split("/").map(_.trim).filter(_.nonEmpty)
          val targetHandle = if (keys.isEmpty) storeHandle else storeHandle.resolve(keys: _*)
          val arr: ZarrArray = ZarrArray.open(targetHandle)

          val shape = arr.metadata.shape.map(_.toInt)
          val chunksOpt = Option(arr.metadata.chunkShape).map(_.map(_.toInt))

          Some((new V3ArrayReader(arr), ArrayMeta(shape, chunksOpt)))
        } catch {
          case t if isNoOpsAllowed(t) || isMissingMeta(t) =>
            recordErr(t)
            None
        }
      }

        val v3Result = openV3()
        v3Result.foreach(return _)

        if (!(forceV3 || hasV3RootMeta || hasV3ArrayMeta)) {
          try {
            return V2ZarrAdapter.open(storeRoot.toString, fullNode, conf)
          } catch {
            case t if isNoOpsAllowed(t) || isMissingMeta(t) =>
              recordErr(t)
          }
        }
      }
    }

    val baseMsg = s"Could not find Zarr metadata. Checked: ${checkedPaths.mkString(", ")}"
    if (lastErr != null) {
      val errMsg = Option(lastErr.getMessage).filter(_.nonEmpty)
      val suffix = errMsg match {
        case Some(m) => s" Last error: ${lastErr.getClass.getName}: $m"
        case None => s" Last error: ${lastErr.getClass.getName}"
      }
      throw new IllegalArgumentException(baseMsg + suffix, lastErr)
    }
    throw new IllegalArgumentException(baseMsg)
  }

  def boolOpt(options: CaseInsensitiveStringMap, key: String, default: Boolean): Boolean =
    Option(options.get(key)).map(_.toBoolean).getOrElse(default)

  def intOpt(options: CaseInsensitiveStringMap, key: String, default: Int): Int =
    Option(options.get(key)).map(_.toInt).getOrElse(default)

  def strOpt(options: CaseInsensitiveStringMap, key: String): Option[String] =
    Option(options.get(key)).map(_.trim).filter(_.nonEmpty)

  def strReq(options: CaseInsensitiveStringMap, key: String): String =
    strOpt(options, key).getOrElse(throw new IllegalArgumentException(s"Missing required option: $key"))

  private def listOpt(options: CaseInsensitiveStringMap, key: String): Seq[String] =
    strOpt(options, key).map(parseList).getOrElse(Seq.empty)

  private def listReqAny(options: CaseInsensitiveStringMap, keys: Seq[String]): Seq[String] = {
    val list = listOptAny(options, keys)
    if (list.isEmpty) throw new IllegalArgumentException(s"Missing required option: ${keys.head}")
    list
  }

  private def aliasList(
      options: CaseInsensitiveStringMap,
      keys: Seq[String],
      count: Int,
      defaultBase: String
  ): Seq[String] = {
    val aliases = listOptAny(options, keys)
    if (count <= 0) return Seq.empty
    if (aliases.isEmpty) {
      if (count == 1) Seq(defaultBase)
      else Seq.tabulate(count)(i => s"${defaultBase}_${i + 1}")
    } else if (aliases.size == 1 && count > 1) {
      val base = aliases.head
      Seq.tabulate(count)(i => s"${base}_${i + 1}")
    } else if (aliases.size != count) {
      val name = keys.headOption.getOrElse("alias")
      throw new IllegalArgumentException(
        s"$name expects $count value(s) when $count node(s) are provided."
      )
    } else {
      aliases
    }
  }

  private def listOptAny(options: CaseInsensitiveStringMap, keys: Seq[String]): Seq[String] = {
    val found = keys.flatMap { key =>
      val list = listOpt(options, key)
      if (list.nonEmpty) Some(key -> list) else None
    }
    if (found.isEmpty) return Seq.empty
    val first = found.head._2
    if (found.exists(_._2 != first)) {
      val names = found.map(_._1).mkString(", ")
      throw new IllegalArgumentException(s"Conflicting options provided: $names")
    }
    first
  }

  private def parseList(value: String): Seq[String] =
    value.split(",").toSeq.map(_.trim).filter(_.nonEmpty)

  /**
   * Databricks Volumes are reliably readable via DBFS paths (dbfs:/Volumes/...).
   * Normalize Volumes paths to include dbfs:/Volumes/... first while keeping
   * the local /Volumes/... path as a fallback for non-DBFS runtimes.
   */
  def normalizeStoreRoots(root: String): Seq[String] = {
    val trimmed = root.trim
    if (trimmed.isEmpty) return Seq.empty

    if (trimmed.startsWith("dbfs:")) {
      return Seq(trimmed)
    }

    val noScheme = if (trimmed.startsWith("file:")) trimmed.stripPrefix("file:") else trimmed
    val normalized =
      if (noScheme.startsWith("/dbfs/")) noScheme.stripPrefix("/dbfs")
      else if (noScheme.startsWith("/Workspace/")) noScheme.stripPrefix("/Workspace")
      else noScheme

    if (normalized.startsWith("/Volumes/") || normalized == "/Volumes") {
      Seq(s"dbfs:$normalized", normalized)
    } else {
      Seq(trimmed)
    }
  }

  private def filesystemForRoot(root: String, conf: Configuration): Option[org.apache.hadoop.fs.FileSystem] = {
    val path = new Path(root)
    try {
      Some(path.getFileSystem(conf))
    } catch {
      case _: UnsupportedFileSystemException => None
    }
  }

  private val stringArrayCache = new ConcurrentHashMap[String, SoftReference[Array[String]]]()
  private val nameIndexCache = new ConcurrentHashMap[String, SoftReference[Map[String, Int]]]()
  private val jsonMapper = new ObjectMapper()

  private def cacheKey(options: CaseInsensitiveStringMap, node: String): String = {
    val roots = normalizeStoreRoots(strReq(options, "path")).mkString("|")
    val fullNode = resolveNodePath(node)
    s"$roots::$fullNode"
  }

  def cachedStringArray(options: CaseInsensitiveStringMap, conf: Configuration, node: String): Array[String] = {
    val key = cacheKey(options, node)
    val cached = Option(stringArrayCache.get(key)).flatMap(ref => Option(ref.get))
    cached.getOrElse {
      val strings = readStringArray(options, conf, node)
      stringArrayCache.put(key, new SoftReference(strings))
      strings
    }
  }

  def cachedNameIndex(options: CaseInsensitiveStringMap, conf: Configuration, node: String): Map[String, Int] = {
    val key = cacheKey(options, node) + "#index"
    val cached = Option(nameIndexCache.get(key)).flatMap(ref => Option(ref.get))
    cached.getOrElse {
      val names = cachedStringArray(options, conf, node)
      val m = scala.collection.mutable.HashMap.empty[String, Int]
      var i = 0
      while (i < names.length) {
        val name = names(i)
        if (name != null && !m.contains(name)) m.put(name, i)
        i += 1
      }
      val map = m.toMap
      nameIndexCache.put(key, new SoftReference(map))
      map
    }
  }

  private def readStringArray(options: CaseInsensitiveStringMap, conf: Configuration, node: String): Array[String] = {
    readStringArrayV3(options, conf, node)
      .orElse(readStringArrayV2(options, conf, node))
      .getOrElse {
        val (reader, meta) = openArray(options, conf, overrideNode = Some(node))
        val shape = meta.shape
        if (shape.isEmpty) return Array.empty

        val offset = Array.fill(shape.length)(0L)
        val readShape = shape.clone()
        val data = reader.read(offset, readShape)
        toStringArray(data, shape)
      }
  }

  private final case class V3StringArrayMeta(
    shape: Array[Int],
    chunkShape: Array[Int],
    fillValue: String,
    codecs: Vector[String],
    chunkKeyEncoding: String,
    chunkKeySeparator: String
  )

  private def readStringArrayV3(options: CaseInsensitiveStringMap, conf: Configuration, node: String): Option[Array[String]] = {
    val roots = normalizeStoreRoots(strReq(options, "path"))
    val fullNode = resolveNodePath(node)
    if (fullNode.isEmpty) return None

    roots.foreach { root =>
      val storeRoot = new Path(root)
      val fsOpt = filesystemForRoot(root, conf)
      if (fsOpt.nonEmpty) {
        val fs = fsOpt.get
        val arrayPath = new Path(storeRoot, fullNode)
        val metaPath = new Path(arrayPath, "zarr.json")

        val metaNode = readJsonNode(fs, metaPath)
        val meta = metaNode.flatMap(parseV3StringArrayMeta)
        meta.foreach { m =>
          return Some(readV3StringArray(fs, arrayPath, m))
        }
      }
    }
    None
  }

  private final case class V2StringArrayMeta(
    shape: Array[Int],
    chunks: Array[Int],
    fillValue: String,
    dtypeKind: Char,
    dtypeLen: Int,
    byteOrder: ByteOrder,
    compressor: Option[V2Compressor],
    dimensionSeparator: String
  )

  private sealed trait V2Compressor extends Serializable
  private final case class V2Blosc() extends V2Compressor
  private final case class V2Zstd() extends V2Compressor
  private final case class V2Gzip() extends V2Compressor
  private final case class V2Zlib() extends V2Compressor

  private def readStringArrayV2(options: CaseInsensitiveStringMap, conf: Configuration, node: String): Option[Array[String]] = {
    val roots = normalizeStoreRoots(strReq(options, "path"))
    val fullNode = resolveNodePath(node)
    if (fullNode.isEmpty) return None

    roots.foreach { root =>
      val storeRoot = new Path(root)
      val fsOpt = filesystemForRoot(root, conf)
      if (fsOpt.nonEmpty) {
        val fs = fsOpt.get
        val arrayPath = new Path(storeRoot, fullNode)
        val metaPath = new Path(arrayPath, ".zarray")

        val metaNode = readJsonNode(fs, metaPath)
        val meta = metaNode.flatMap(parseV2StringArrayMeta)
        meta.foreach { m =>
          return Some(readV2StringArray(fs, arrayPath, m))
        }
      }
    }
    None
  }

  private def parseV2StringArrayMeta(root: JsonNode): Option[V2StringArrayMeta] = {
    val zarrFormat = root.path("zarr_format").asInt(-1)
    if (zarrFormat != 2) return None

    val dtype = root.path("dtype").asText("")
    val dtypeMatch = """([<>|=]?)([US])(\d+)""".r
    val (orderChar, kind, len) = dtype match {
      case dtypeMatch(order, k, n) => (order, k.charAt(0), n.toInt)
      case _ => return None
    }

    if (len <= 0) return None

    val shape = readIntArray(root.path("shape"))
    val chunks = readIntArray(root.path("chunks"))
    if (shape.length != 1 || chunks.length != 1) {
      throw new IllegalArgumentException(s"Only 1D string arrays are supported for v2 (got shape ${shape.length}D).")
    }

    val filters = root.path("filters")
    if (filters.isArray && filters.size() > 0) {
      throw new IllegalArgumentException("filters are not supported for v2 string arrays.")
    }

    val fillValueNode = root.path("fill_value")
    val fillValue = if (fillValueNode.isMissingNode || fillValueNode.isNull) "" else fillValueNode.asText("")

    val compressorNode = root.path("compressor")
    val compressor = if (compressorNode.isMissingNode || compressorNode.isNull) {
      None
    } else {
      compressorNode.path("id").asText("") match {
        case "blosc" => Some(V2Blosc())
        case "zstd" => Some(V2Zstd())
        case "gzip" => Some(V2Gzip())
        case "zlib" => Some(V2Zlib())
        case other =>
          throw new IllegalArgumentException(s"Unsupported compressor for v2 string arrays: $other")
      }
    }

    val dimSep = root.path("dimension_separator").asText(".")
    val byteOrder = orderChar match {
      case ">" => ByteOrder.BIG_ENDIAN
      case _ => ByteOrder.LITTLE_ENDIAN
    }

    Some(V2StringArrayMeta(shape, chunks, fillValue, kind, len, byteOrder, compressor, dimSep))
  }

  private def readV2StringArray(
    fs: org.apache.hadoop.fs.FileSystem,
    arrayPath: Path,
    meta: V2StringArrayMeta
  ): Array[String] = {
    val total = meta.shape.headOption.getOrElse(0)
    if (total == 0) return Array.empty

    val chunkSize = meta.chunks.headOption.getOrElse(total)
    val out = Array.fill(total)(meta.fillValue)
    val numChunks = (total + chunkSize - 1) / chunkSize

    var chunk = 0
    while (chunk < numChunks) {
      val key = chunk.toString
      val chunkPath =
        if (meta.dimensionSeparator == "/") new Path(arrayPath, key)
        else new Path(arrayPath, key)

      if (fs.exists(chunkPath)) {
        val raw = readAllBytes(fs, chunkPath)
        val decoded = decodeV2StringChunk(raw, meta)
        val offset = chunk * chunkSize
        val len = math.min(chunkSize, total - offset)
        var i = 0
        while (i < len && i < decoded.length) {
          out(offset + i) = decoded(i)
          i += 1
        }
      }
      chunk += 1
    }
    out
  }

  private def decodeV2StringChunk(raw: Array[Byte], meta: V2StringArrayMeta): Array[String] = {
    val bytes = meta.compressor match {
      case None => raw
      case Some(V2Blosc()) => com.scalableminds.bloscjava.Blosc.decompress(raw)
      case Some(V2Zstd()) => zstdDecompress(raw)
      case Some(V2Gzip()) => gzipDecompress(raw)
      case Some(V2Zlib()) => zlibDecompress(raw)
    }

    meta.dtypeKind match {
      case 'S' => decodeFixedBytes(bytes, meta.dtypeLen)
      case 'U' => decodeFixedUnicode(bytes, meta.dtypeLen, meta.byteOrder)
      case other => throw new IllegalArgumentException(s"Unsupported v2 string dtype kind: $other")
    }
  }

  private def decodeFixedBytes(data: Array[Byte], width: Int): Array[String] = {
    if (width <= 0) return Array.empty
    val count = data.length / width
    val out = new Array[String](count)
    var i = 0
    while (i < count) {
      val start = i * width
      val end = start + width
      var trim = end
      var j = start
      while (j < end) {
        if (data(j) == 0) {
          trim = j
          j = end
        } else {
          j += 1
        }
      }
      out(i) = new String(data, start, trim - start, StandardCharsets.UTF_8).trim
      i += 1
    }
    out
  }

  private def decodeFixedUnicode(data: Array[Byte], width: Int, order: ByteOrder): Array[String] = {
    if (width <= 0) return Array.empty
    val bytesPerElem = width * 4
    val count = data.length / bytesPerElem
    val out = new Array[String](count)
    val buf = ByteBuffer.wrap(data).order(order)
    var i = 0
    while (i < count) {
      val sb = new StringBuilder
      var j = 0
      while (j < width && buf.remaining() >= 4) {
        val cp = buf.getInt
        if (cp != 0 && Character.isValidCodePoint(cp)) {
          sb.append(new String(Character.toChars(cp)))
        }
        j += 1
      }
      out(i) = sb.toString
      i += 1
    }
    out
  }

  private def readJsonNode(fs: org.apache.hadoop.fs.FileSystem, path: Path): Option[JsonNode] = {
    var in: org.apache.hadoop.fs.FSDataInputStream = null
    try {
      if (!fs.exists(path)) return None
      in = fs.open(path)
      Some(jsonMapper.readTree(in))
    } catch {
      case _: java.io.FileNotFoundException => None
      case _: java.nio.file.NoSuchFileException => None
    } finally {
      if (in != null) in.close()
    }
  }

  private def parseV3StringArrayMeta(root: JsonNode): Option[V3StringArrayMeta] = {
    val zarrFormat = root.path("zarr_format").asInt(-1)
    val nodeType = root.path("node_type").asText("")
    val dataType = root.path("data_type").asText("")
    if (zarrFormat != 3 || nodeType != "array" || dataType != "string") return None

    val shape = readIntArray(root.path("shape"))
    if (shape.isEmpty) return Some(V3StringArrayMeta(shape, shape, "", Vector.empty, "default", "/"))
    if (shape.length != 1) {
      throw new IllegalArgumentException(s"Only 1D string arrays are supported (got ${shape.length}D).")
    }

    val grid = root.path("chunk_grid")
    val gridName = grid.path("name").asText("")
    if (gridName != "regular") {
      throw new IllegalArgumentException(s"Only regular chunk grid is supported for string arrays (got '$gridName').")
    }
    val chunkShape = readIntArray(grid.path("configuration").path("chunk_shape"))
    if (chunkShape.length != shape.length) {
      throw new IllegalArgumentException("chunk_shape rank does not match shape rank for string array.")
    }

    val codecs = root.path("codecs")
    val codecNames = if (codecs.isArray) {
      codecs.elements().asScala.toVector.map(_.path("name").asText(""))
    } else Vector.empty

    val fillValueNode = root.path("fill_value")
    val fillValue = if (fillValueNode.isMissingNode || fillValueNode.isNull) "" else fillValueNode.asText("")

    val storageTransformers = root.path("storage_transformers")
    if (storageTransformers.isArray && storageTransformers.size() > 0) {
      throw new IllegalArgumentException("storage_transformers are not supported for string arrays.")
    }

    val keyEnc = root.path("chunk_key_encoding")
    val keyEncName = keyEnc.path("name").asText("default")
    val sep = keyEnc.path("configuration").path("separator").asText("/")

    Some(V3StringArrayMeta(shape, chunkShape, fillValue, codecNames, keyEncName, sep))
  }

  private def readV3StringArray(
    fs: org.apache.hadoop.fs.FileSystem,
    arrayPath: Path,
    meta: V3StringArrayMeta
  ): Array[String] = {
    val total = meta.shape.headOption.getOrElse(0)
    if (total == 0) return Array.empty

    val chunkSize = meta.chunkShape.headOption.getOrElse(total)
    val out = Array.fill(total)(meta.fillValue)

    val numChunks = (total + chunkSize - 1) / chunkSize
    var chunk = 0
    while (chunk < numChunks) {
      val key = chunkKey(meta.chunkKeyEncoding, meta.chunkKeySeparator, Array(chunk))
      val chunkPath = new Path(arrayPath, key)
      if (fs.exists(chunkPath)) {
        val raw = readAllBytes(fs, chunkPath)
        val decoded = decodeV3StringChunk(raw, meta.codecs)
        val offset = chunk * chunkSize
        val len = math.min(chunkSize, total - offset)
        var i = 0
        while (i < len && i < decoded.length) {
          out(offset + i) = decoded(i)
          i += 1
        }
      }
      chunk += 1
    }
    out
  }

  private def decodeV3StringChunk(raw: Array[Byte], codecs: Vector[String]): Array[String] = {
    val supported = Set("vlen-utf8", "zstd")
    codecs.foreach { name =>
      if (!supported.contains(name)) {
        throw new IllegalArgumentException(s"Unsupported codec for string arrays: $name")
      }
    }
    if (!codecs.contains("vlen-utf8")) {
      throw new IllegalArgumentException("Missing vlen-utf8 codec for string array.")
    }

    var bytes = raw
    codecs.reverse.foreach {
      case "zstd" => bytes = zstdDecompress(bytes)
      case "vlen-utf8" => ()
      case _ => ()
    }
    decodeVlenUtf8(bytes)
  }

  private def decodeVlenUtf8(data: Array[Byte]): Array[String] = {
    if (data.length < 4) return Array.empty
    val buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN)
    val count = buf.getInt
    if (count <= 0) return Array.empty
    val out = new Array[String](count)
    var i = 0
    while (i < count && buf.remaining() >= 4) {
      val len = buf.getInt
      if (len < 0 || buf.remaining() < len) {
        throw new IllegalArgumentException(s"Invalid vlen-utf8 length $len at index $i.")
      }
      val bytes = new Array[Byte](len)
      buf.get(bytes)
      out(i) = new String(bytes, StandardCharsets.UTF_8)
      i += 1
    }
    out
  }

  private def zstdDecompress(data: Array[Byte]): Array[Byte] = {
    val in = new ZstdInputStream(new ByteArrayInputStream(data))
    val out = new ByteArrayOutputStream()
    val buf = new Array[Byte](64 * 1024)
    var n = in.read(buf)
    while (n > 0) {
      out.write(buf, 0, n)
      n = in.read(buf)
    }
    in.close()
    out.toByteArray
  }

  private def gzipDecompress(data: Array[Byte]): Array[Byte] = {
    val in = new GZIPInputStream(new ByteArrayInputStream(data))
    val out = new ByteArrayOutputStream()
    val buf = new Array[Byte](64 * 1024)
    var n = in.read(buf)
    while (n > 0) {
      out.write(buf, 0, n)
      n = in.read(buf)
    }
    in.close()
    out.toByteArray
  }

  private def zlibDecompress(data: Array[Byte]): Array[Byte] = {
    val in = new InflaterInputStream(new ByteArrayInputStream(data))
    val out = new ByteArrayOutputStream()
    val buf = new Array[Byte](64 * 1024)
    var n = in.read(buf)
    while (n > 0) {
      out.write(buf, 0, n)
      n = in.read(buf)
    }
    in.close()
    out.toByteArray
  }

  private def readAllBytes(fs: org.apache.hadoop.fs.FileSystem, path: Path): Array[Byte] = {
    val in = fs.open(path)
    val out = new ByteArrayOutputStream()
    val buf = new Array[Byte](64 * 1024)
    var n = in.read(buf)
    while (n > 0) {
      out.write(buf, 0, n)
      n = in.read(buf)
    }
    in.close()
    out.toByteArray
  }

  private def readIntArray(node: JsonNode): Array[Int] = {
    if (!node.isArray) return Array.empty
    node.elements().asScala.map { n =>
      val v = n.asLong()
      if (v > Int.MaxValue || v < Int.MinValue) {
        throw new IllegalArgumentException(s"Array dimension out of Int range: $v")
      }
      v.toInt
    }.toArray
  }

  private def chunkKey(encoding: String, separator: String, indices: Array[Int]): String = {
    encoding match {
      case "default" =>
        val body = indices.map(_.toString).mkString(separator)
        s"c$separator$body"
      case other =>
        throw new IllegalArgumentException(s"Unsupported chunk_key_encoding for string arrays: $other")
    }
  }

  private def toStringArray(arr: ma2.Array, shape: Array[Int]): Array[String] = {
    val rank = shape.length
    arr.getDataType match {
      case ma2.DataType.CHAR if rank >= 2 =>
        val n = shape(0)
        val width = shape(1)
        val out = new Array[String](n)
        val it = arr.getIndexIterator
        var i = 0
        while (i < n) {
          val chars = new Array[Char](width)
          var j = 0
          while (j < width) {
            val obj = it.getObjectNext
            val ch = obj match {
              case c: Character => c.charValue
              case b: java.lang.Byte => b.toChar
              case s: String if s.nonEmpty => s.charAt(0)
              case _ => 0.toChar
            }
            chars(j) = ch
            j += 1
          }
          out(i) = new String(chars).trim
          i += 1
        }
        out

      case dt if (rank >= 2 && (dt == ma2.DataType.BYTE || dt == ma2.DataType.UBYTE)) =>
        val n = shape(0)
        val width = shape(1)
        val out = new Array[String](n)
        val it = arr.getIndexIterator
        var i = 0
        while (i < n) {
          val bytes = new Array[Byte](width)
          var j = 0
          while (j < width) {
            val obj = it.getObjectNext
            val b = obj match {
              case v: java.lang.Byte => v.byteValue
              case v: java.lang.Short => v.shortValue.toByte
              case v: java.lang.Integer => v.intValue.toByte
              case v: java.lang.Long => v.longValue.toByte
              case _ => 0.toByte
            }
            bytes(j) = b
            j += 1
          }
          out(i) = new String(bytes, StandardCharsets.UTF_8).trim
          i += 1
        }
        out

      case ma2.DataType.CHAR =>
        val it = arr.getIndexIterator
        val chars = new Array[Char](shape.headOption.getOrElse(0))
        var i = 0
        while (it.hasNext) {
          val obj = it.getObjectNext
          val ch = obj match {
            case c: Character => c.charValue
            case b: java.lang.Byte => b.toChar
            case s: String if s.nonEmpty => s.charAt(0)
            case _ => 0.toChar
          }
          if (i < chars.length) {
            chars(i) = ch
          }
          i += 1
        }
        Array(new String(chars).trim)

      case ma2.DataType.STRING =>
        val size = arr.getSize.toInt
        val out = new Array[String](size)
        val it = arr.getIndexIterator
        var i = 0
        while (it.hasNext) {
          val obj = it.getObjectNext
          out(i) = if (obj == null) null else obj.toString
          i += 1
        }
        out

      case _ =>
        val size = arr.getSize.toInt
        val out = new Array[String](size)
        val it = arr.getIndexIterator
        var i = 0
        while (it.hasNext) {
          val obj = it.getObjectNext
          out(i) = if (obj == null) null else obj.toString
          i += 1
        }
        out
    }
  }

  private final class V3ArrayReader(arr: ZarrArray) extends ArrayReader {
    override def shape: Array[Int] = arr.metadata.shape.map(_.toInt)
    override def chunkShape: Option[Array[Int]] = Option(arr.metadata.chunkShape).map(_.map(_.toInt))
    override def read(offset: Array[Long], readShape: Array[Int]): ma2.Array =
      arr.read(offset, readShape)
  }
}

/** Filter parsing / pruning logic */
object ZarrFilters {

  def isSupportedForPruning(f: Filter): Boolean = f match {
    case EqualTo(attr, _) if supportedAttr(attr, Seq("indexes"), Seq("columns")) => true
    case In(attr, _) if supportedAttr(attr, Seq("indexes"), Seq("columns")) => true
    case _ => false
  }

  def isSupportedForPruning(f: Filter, options: CaseInsensitiveStringMap): Boolean = {
    val (columnsNodes, indexNodes) = ZarrUtils.requireIndexAndColumns(options)
    val (columnAttrs, indexAttrs) = ZarrUtils.requireAliases(options, columnsNodes, indexNodes)
    f match {
      case EqualTo(attr, _) if supportedAttr(attr, indexAttrs, columnAttrs) => true
      case In(attr, _) if supportedAttr(attr, indexAttrs, columnAttrs) => true
      case _ => false
    }
  }

  private def supportedAttr(attr: String, indexAttrs: Seq[String], columnAttrs: Seq[String]): Boolean =
    indexAttrs.contains(attr) || columnAttrs.contains(attr)

  def toPruneSpec(
    filters: Array[Filter],
    options: CaseInsensitiveStringMap,
    conf: Configuration,
    columnsPerBlock: Int
  ): PruneSpec = {
    var min: Option[Long] = None
    var max: Option[Long] = None
    var columnStarts: Option[Set[Int]] = None
    val columnNamesByAlias = scala.collection.mutable.Map.empty[String, Set[String]]
    val indexNamesByAlias = scala.collection.mutable.Map.empty[String, Set[String]]

    val blockSize = Math.max(1, columnsPerBlock)
    val (columnsNodes, indexNodes) = ZarrUtils.requireIndexAndColumns(options)
    val (columnAliases, indexAliases) = ZarrUtils.requireAliases(options, columnsNodes, indexNodes)
    val columnAliasToNode = columnAliases.zip(columnsNodes).toMap
    val indexAliasToNode = indexAliases.zip(indexNodes).toMap

    def tightenMin(v: Long): Unit = min = Some(min.fold(v)(m => Math.max(m, v)))
    def tightenMax(v: Long): Unit = max = Some(max.fold(v)(m => Math.min(m, v)))
    def updateNames(target: scala.collection.mutable.Map[String, Set[String]], alias: String, incoming: Set[String]): Unit = {
      val next = target.get(alias).fold(incoming)(_ intersect incoming)
      target.update(alias, next)
    }

    filters.foreach {
      case EqualTo(attr, v) if indexAliasToNode.contains(attr) =>
        val s = toStr(v)
        updateNames(indexNamesByAlias, attr, Set(s))

      case In(attr, vs) if indexAliasToNode.contains(attr) =>
        val ss = vs.map(toStr).toSet
        updateNames(indexNamesByAlias, attr, ss)

      case EqualTo(attr, v) if columnAliasToNode.contains(attr) =>
        val s = toStr(v)
        updateNames(columnNamesByAlias, attr, Set(s))

      case In(attr, vs) if columnAliasToNode.contains(attr) =>
        val ss = vs.map(toStr).toSet
        updateNames(columnNamesByAlias, attr, ss)

      case _ => ()
    }

    val indexSets = indexNamesByAlias.toSeq.map { case (alias, names) =>
      val node = indexAliasToNode(alias)
      val nameIndex = ZarrUtils.cachedNameIndex(options, conf, node)
      names.flatMap(nameIndex.get).toSet
    }

    val columnSets = columnNamesByAlias.toSeq.map { case (alias, names) =>
      val node = columnAliasToNode(alias)
      val nameIndex = ZarrUtils.cachedNameIndex(options, conf, node)
      names.flatMap(nameIndex.get).toSet
    }

    val indexIndices = indexSets.reduceOption(_ intersect _)
    indexIndices.foreach { indices =>
      if (indices.isEmpty) {
        min = Some(Long.MaxValue)
        max = Some(Long.MinValue)
      } else {
        val longs = indices.map(_.toLong)
        tightenMin(longs.min)
        tightenMax(longs.max)
      }
    }

    val columnIndices = columnSets.reduceOption(_ intersect _)
    columnIndices.foreach { indices =>
      if (indices.isEmpty) {
        columnStarts = Some(Set.empty)
      } else {
        val starts = indices.map(i => (i / blockSize) * blockSize).toSet
        columnStarts = Some(columnStarts.fold(starts)(_ intersect starts))
      }
    }

    PruneSpec(indexMin = min, indexMax = max, columnBlockStarts = columnStarts)
  }

  private def toStr(v: Any): String = v match {
    case s: String => s
    case other => other.toString
  }
}
