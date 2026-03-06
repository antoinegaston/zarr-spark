package com.epigene.zarr

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, UnsupportedFileSystemException}
import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters._
import scala.util.Using

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}
import java.lang.ref.SoftReference
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.concurrent.ConcurrentHashMap
import java.util.zip.{GZIPInputStream, InflaterInputStream}

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.github.luben.zstd.ZstdInputStream
import dev.zarr.zarrjava.store.{Store, StoreHandle}
import dev.zarr.zarrjava.v2.{Array => LegacyZarrArray}
import dev.zarr.zarrjava.v3.{Array => ZarrArray}
import ucar.ma2

object ZarrUtils {

  // ---------------------------------------------------------------------------
  // Public types
  // ---------------------------------------------------------------------------

  trait ArrayReader {
    def read(offset: Array[Long], shape: Array[Int]): ma2.Array
    def shape: Array[Int]
    def chunkShape: Option[Array[Int]]
  }

  final case class ArrayMeta(shape: Array[Int], chunkShape: Option[Array[Int]])

  // ---------------------------------------------------------------------------
  // Hadoop / Spark configuration
  // ---------------------------------------------------------------------------

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

  // ---------------------------------------------------------------------------
  // Options helpers
  // ---------------------------------------------------------------------------

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

  private def listReq(options: CaseInsensitiveStringMap, key: String): Seq[String] = {
    val list = listOpt(options, key)
    if (list.isEmpty) throw new IllegalArgumentException(s"Missing required option: $key")
    list
  }

  private def parseList(value: String): Seq[String] =
    value.split(",").toSeq.map(_.trim).filter(_.nonEmpty)

  // ---------------------------------------------------------------------------
  // Layer / axis / alias helpers
  // ---------------------------------------------------------------------------

  def layerSpec(options: CaseInsensitiveStringMap): LayerSpec = {
    val nodePath = strReq(options, "valuesNode")
    if (nodePath.contains(",")) {
      throw new IllegalArgumentException("Option 'valuesNode' expects a single node path.")
    }
    NodeLayerSpec(nodePath)
  }

  /**
   * Returns (indexSize, columnsSize).
   * Normalizes to a logical 2D view [index x columns] per values node.
   */
  def requireShape2D(shape: Array[Int]): (Int, Int) = {
    if (shape.length != 2) {
      throw new IllegalArgumentException(s"Values node expects 2D arrays, got ${shape.length}D")
    }
    (shape(0), shape(1))
  }

  def requireColumnsAndIndex(options: CaseInsensitiveStringMap): (Seq[String], Seq[String]) = {
    val columns = listReq(options, "columnsNodes")
    val index = listReq(options, "indexNodes")
    (columns, index)
  }

  def requireAliases(
      options: CaseInsensitiveStringMap,
      columnsNodes: Seq[String],
      indexNodes: Seq[String]
  ): (Seq[String], Seq[String]) = {
    val columnAliases = aliasList(options, "columnAliases", columnsNodes.size, defaultBase = "columns")
    val indexAliases = aliasList(options, "indexAliases", indexNodes.size, defaultBase = "indexes")
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

  private def aliasList(
      options: CaseInsensitiveStringMap,
      key: String,
      count: Int,
      defaultBase: String
  ): Seq[String] = {
    val aliases = listOpt(options, key)
    if (count <= 0) return Seq.empty
    if (aliases.isEmpty) {
      if (count == 1) Seq(defaultBase)
      else Seq.tabulate(count)(i => s"${defaultBase}_${i + 1}")
    } else if (aliases.size == 1 && count > 1) {
      val base = aliases.head
      Seq.tabulate(count)(i => s"${base}_${i + 1}")
    } else if (aliases.size != count) {
      throw new IllegalArgumentException(
        s"$key expects $count value(s) when $count node(s) are provided."
      )
    } else {
      aliases
    }
  }

  // ---------------------------------------------------------------------------
  // Store / path resolution
  // ---------------------------------------------------------------------------

  def resolveNodePath(node: String): String = {
    val cleanNode = Option(node).getOrElse("").trim
    if (cleanNode.isEmpty) "" else cleanNode.stripPrefix("/").stripSuffix("/")
  }

  /**
   * Databricks Volumes are reliably readable via DBFS paths (dbfs:/Volumes/...).
   * Normalize Volumes paths to include dbfs:/Volumes/... first while keeping
   * the local /Volumes/... path as a fallback for non-DBFS runtimes.
   */
  def normalizeStoreRoots(root: String): Seq[String] = {
    val trimmed = root.trim
    if (trimmed.isEmpty) return Seq.empty

    if (trimmed.startsWith("dbfs:")) return Seq(trimmed)

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

  private def localPathForRoot(root: String): Option[java.nio.file.Path] = {
    val trimmed = root.trim
    if (trimmed.isEmpty) return None
    try {
      if (trimmed.startsWith("file:")) Some(Paths.get(new java.net.URI(trimmed)))
      else if (trimmed.startsWith("/")) Some(Paths.get(trimmed))
      else None
    } catch {
      case _: Exception => None
    }
  }

  private def filesystemForRoot(root: String, conf: Configuration): Option[org.apache.hadoop.fs.FileSystem] = {
    try Some(new Path(root).getFileSystem(conf))
    catch { case _: UnsupportedFileSystemException => None }
  }

  // ---------------------------------------------------------------------------
  // Array opening (v3 / v2 fallback)
  // ---------------------------------------------------------------------------

  def openArray(
      options: CaseInsensitiveStringMap,
      conf: Configuration,
      overrideNode: Option[String]
  ): (ArrayReader, ArrayMeta) = {
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

    def isNoOpsAllowed(t: Throwable): Boolean = t match {
      case _: UnsupportedOperationException                               => true
      case r: java.rmi.RemoteException => Option(r.getCause).exists(isNoOpsAllowed)
      case _                                                              => false
    }

    def isMissingMeta(t: Throwable): Boolean = t match {
      case _: java.io.FileNotFoundException  => true
      case _: java.nio.file.NoSuchFileException => true
      case r: java.rmi.RemoteException       => Option(r.getCause).exists(isMissingMeta)
      case ioe: java.io.IOException =>
        Option(ioe.getMessage).exists(_.toLowerCase.contains("zarray"))
      case iae: IllegalArgumentException =>
        Option(iae.getMessage).exists(_.toLowerCase.contains("zarray"))
      case _ => false
    }

    val result = roots.view.flatMap { root =>
      val storeRoot = new Path(root)
      filesystemForRoot(root, conf) match {
        case None =>
          recordErr(new UnsupportedFileSystemException(s"No FileSystem for scheme '${storeRoot.toUri.getScheme}'"))
          None

        case Some(fs) =>
          val arrayPath = if (fullNode.nonEmpty) new Path(storeRoot, fullNode) else storeRoot
          val rootMetaPath = new Path(storeRoot, "zarr.json")
          val arrayMetaPath = new Path(arrayPath, "zarr.json")
          val legacyMetaPath = new Path(arrayPath, ".zarray")

          checkedPaths += rootMetaPath.toString
          checkedPaths += arrayMetaPath.toString
          checkedPaths += legacyMetaPath.toString

          val localRoot = localPathForRoot(root)
          val localRootMetaExists = localRoot.exists(p => Files.exists(p.resolve("zarr.json")))
          val localArrayMetaExists = localRoot.exists { p =>
            fullNode.nonEmpty && Files.exists(p.resolve(fullNode).resolve("zarr.json"))
          }

          val hasRootMeta = localRootMetaExists || (try fs.exists(rootMetaPath) catch {
            case t if isNoOpsAllowed(t) => false
          })
          val hasArrayMeta = localArrayMetaExists || (try fs.exists(arrayMetaPath) catch {
            case t if isNoOpsAllowed(t) => false
          })

          def tryPrimary(): Option[(ArrayReader, ArrayMeta)] = {
            try {
              val store: Store = new HadoopZarrStore(root, conf)
              val storeHandle = new StoreHandle(store)
              val keys = fullNode.split("/").map(_.trim).filter(_.nonEmpty)
              val targetHandle = if (keys.isEmpty) storeHandle else storeHandle.resolve(keys: _*)
              val arr: ZarrArray = ZarrArray.open(targetHandle)
              val shape = arr.metadata.shape.map(_.toInt)
              val chunksOpt = Option(arr.metadata.chunkShape).map(_.map(_.toInt))
              Some((new ZarrArrayReader(arr), ArrayMeta(shape, chunksOpt)))
            } catch {
              case t if isNoOpsAllowed(t) || isMissingMeta(t) =>
                recordErr(t)
                None
            }
          }

          def tryFallback(): Option[(ArrayReader, ArrayMeta)] = {
            try {
              val store: Store = new HadoopZarrStore(root, conf)
              val storeHandle = new StoreHandle(store)
              val keys = fullNode.split("/").map(_.trim).filter(_.nonEmpty)
              val targetHandle = if (keys.isEmpty) storeHandle else storeHandle.resolve(keys: _*)
              val arr = LegacyZarrArray.open(targetHandle)
              val shape = arr.metadata.shape.map(_.toInt)
              val chunks = Option(arr.metadata.chunkShape()).map(_.map(_.toInt))
              Some((new LegacyArrayReader(arr), ArrayMeta(shape, chunks)))
            } catch {
              case t if isNoOpsAllowed(t) || isMissingMeta(t) =>
                recordErr(t)
                None
            }
          }

          tryPrimary().orElse {
            if (forceV3 || hasRootMeta || hasArrayMeta) None
            else tryFallback()
          }
      }
    }.headOption

    result.getOrElse {
      val baseMsg = s"Could not find Zarr metadata. Checked: ${checkedPaths.mkString(", ")}"
      if (lastErr != null) {
        val errMsg = Option(lastErr.getMessage).filter(_.nonEmpty)
        val suffix = errMsg match {
          case Some(m) => s" Last error: ${lastErr.getClass.getName}: $m"
          case None    => s" Last error: ${lastErr.getClass.getName}"
        }
        throw new IllegalArgumentException(baseMsg + suffix, lastErr)
      }
      throw new IllegalArgumentException(baseMsg)
    }
  }

  // ---------------------------------------------------------------------------
  // Name array caching
  // ---------------------------------------------------------------------------

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

  // ---------------------------------------------------------------------------
  // String array reading (V3)
  // ---------------------------------------------------------------------------

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

  private def readStringArrayV3(
      options: CaseInsensitiveStringMap,
      conf: Configuration,
      node: String
  ): Option[Array[String]] = {
    val roots = normalizeStoreRoots(strReq(options, "path"))
    val fullNode = resolveNodePath(node)
    if (fullNode.isEmpty) return None

    roots.foreach { root =>
      val storeRoot = new Path(root)
      filesystemForRoot(root, conf).foreach { fs =>
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
      case _      => ()
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

  // ---------------------------------------------------------------------------
  // String array reading (V2)
  // ---------------------------------------------------------------------------

  private final case class V2StringArrayMeta(
      shape: Array[Int],
      chunks: Array[Int],
      fillValue: String,
      dtypeKind: Char,
      dtypeLen: Int,
      byteOrder: ByteOrder,
      compressor: Option[V2Compressor],
      dimensionSeparator: String,
      vlenUtf8: Boolean = false
  )

  private sealed trait V2Compressor extends Serializable
  private case object V2Blosc extends V2Compressor
  private case object V2Zstd extends V2Compressor
  private case object V2Gzip extends V2Compressor
  private case object V2Zlib extends V2Compressor

  private def readStringArrayV2(
      options: CaseInsensitiveStringMap,
      conf: Configuration,
      node: String
  ): Option[Array[String]] = {
    val roots = normalizeStoreRoots(strReq(options, "path"))
    val fullNode = resolveNodePath(node)
    if (fullNode.isEmpty) return None

    roots.foreach { root =>
      val storeRoot = new Path(root)
      filesystemForRoot(root, conf).foreach { fs =>
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

    val filters = root.path("filters")
    val filterIds = if (filters.isArray) {
      filters.elements().asScala.toVector.map(_.path("id").asText(""))
    } else Vector.empty
    val hasVlenUtf8 = filterIds.contains("vlen-utf8")
    val unsupportedFilters = filterIds.filterNot(_ == "vlen-utf8")
    if (unsupportedFilters.nonEmpty) {
      throw new IllegalArgumentException(
        s"Unsupported filter(s) for v2 string arrays: ${unsupportedFilters.mkString(", ")}"
      )
    }

    val isObjectDtype = dtype == "|O" || dtype == "object"
    val dtypeMatch = """([<>|=]?)([US])(\d+)""".r

    val (orderChar, kind, len, vlen) = if (isObjectDtype && hasVlenUtf8) {
      ("|", 'O', 0, true)
    } else {
      dtype match {
        case dtypeMatch(order, k, n) if n.toInt > 0 =>
          if (hasVlenUtf8) {
            throw new IllegalArgumentException(
              "vlen-utf8 filter is only expected with object dtype, not fixed-width string dtype."
            )
          }
          (order, k.charAt(0), n.toInt, false)
        case _ => return None
      }
    }

    val shape = readIntArray(root.path("shape"))
    val chunks = readIntArray(root.path("chunks"))
    if (shape.length != 1 || chunks.length != 1) {
      throw new IllegalArgumentException(s"Only 1D string arrays are supported for v2 (got shape ${shape.length}D).")
    }

    val fillValueNode = root.path("fill_value")
    val fillValue = if (fillValueNode.isMissingNode || fillValueNode.isNull) "" else fillValueNode.asText("")

    val compressorNode = root.path("compressor")
    val compressor = if (compressorNode.isMissingNode || compressorNode.isNull) {
      None
    } else {
      compressorNode.path("id").asText("") match {
        case "blosc" => Some(V2Blosc)
        case "zstd"  => Some(V2Zstd)
        case "gzip"  => Some(V2Gzip)
        case "zlib"  => Some(V2Zlib)
        case other =>
          throw new IllegalArgumentException(s"Unsupported compressor for v2 string arrays: $other")
      }
    }

    val dimSep = root.path("dimension_separator").asText(".")
    val byteOrder = orderChar match {
      case ">" => ByteOrder.BIG_ENDIAN
      case _   => ByteOrder.LITTLE_ENDIAN
    }

    Some(V2StringArrayMeta(shape, chunks, fillValue, kind, len, byteOrder, compressor, dimSep, vlen))
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
      case None         => raw
      case Some(V2Blosc) => com.scalableminds.bloscjava.Blosc.decompress(raw)
      case Some(V2Zstd)  => zstdDecompress(raw)
      case Some(V2Gzip)  => gzipDecompress(raw)
      case Some(V2Zlib)  => zlibDecompress(raw)
    }

    if (meta.vlenUtf8) return decodeVlenUtf8(bytes)

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

  // ---------------------------------------------------------------------------
  // Generic array → String conversion
  // ---------------------------------------------------------------------------

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
            chars(j) = obj match {
              case c: Character              => c.charValue
              case b: java.lang.Byte         => b.toChar
              case s: String if s.nonEmpty    => s.charAt(0)
              case _                         => 0.toChar
            }
            j += 1
          }
          out(i) = new String(chars).trim
          i += 1
        }
        out

      case dt if rank >= 2 && (dt == ma2.DataType.BYTE || dt == ma2.DataType.UBYTE) =>
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
            bytes(j) = obj match {
              case v: java.lang.Byte    => v.byteValue
              case v: java.lang.Short   => v.shortValue.toByte
              case v: java.lang.Integer => v.intValue.toByte
              case v: java.lang.Long    => v.longValue.toByte
              case _                    => 0.toByte
            }
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
            case c: Character              => c.charValue
            case b: java.lang.Byte         => b.toChar
            case s: String if s.nonEmpty    => s.charAt(0)
            case _                         => 0.toChar
          }
          if (i < chars.length) chars(i) = ch
          i += 1
        }
        Array(new String(chars).trim)

      case ma2.DataType.STRING =>
        val size = arr.getSize.toInt
        val out = new Array[String](size)
        val it = arr.getIndexIterator
        var i = 0
        while (it.hasNext) {
          out(i) = Option(it.getObjectNext).fold(null: String)(_.toString)
          i += 1
        }
        out

      case _ =>
        val size = arr.getSize.toInt
        val out = new Array[String](size)
        val it = arr.getIndexIterator
        var i = 0
        while (it.hasNext) {
          out(i) = Option(it.getObjectNext).fold(null: String)(_.toString)
          i += 1
        }
        out
    }
  }

  // ---------------------------------------------------------------------------
  // I/O helpers
  // ---------------------------------------------------------------------------

  private def drainToBytes(in: InputStream): Array[Byte] = {
    try {
      val out = new ByteArrayOutputStream()
      val buf = new Array[Byte](64 * 1024)
      var n = in.read(buf)
      while (n > 0) {
        out.write(buf, 0, n)
        n = in.read(buf)
      }
      out.toByteArray
    } finally {
      in.close()
    }
  }

  private def zstdDecompress(data: Array[Byte]): Array[Byte] =
    drainToBytes(new ZstdInputStream(new ByteArrayInputStream(data)))

  private def gzipDecompress(data: Array[Byte]): Array[Byte] =
    drainToBytes(new GZIPInputStream(new ByteArrayInputStream(data)))

  private def zlibDecompress(data: Array[Byte]): Array[Byte] =
    drainToBytes(new InflaterInputStream(new ByteArrayInputStream(data)))

  private def readAllBytes(fs: org.apache.hadoop.fs.FileSystem, path: Path): Array[Byte] =
    drainToBytes(fs.open(path))

  private def readJsonNode(fs: org.apache.hadoop.fs.FileSystem, path: Path): Option[JsonNode] = {
    try {
      if (!fs.exists(path)) return None
      Using.resource(fs.open(path)) { in =>
        Some(jsonMapper.readTree(in))
      }
    } catch {
      case _: java.io.FileNotFoundException | _: java.nio.file.NoSuchFileException => None
    }
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

  // ---------------------------------------------------------------------------
  // Array readers
  // ---------------------------------------------------------------------------

  private final class ZarrArrayReader(arr: ZarrArray) extends ArrayReader {
    override def shape: Array[Int] = arr.metadata.shape.map(_.toInt)
    override def chunkShape: Option[Array[Int]] = Option(arr.metadata.chunkShape).map(_.map(_.toInt))
    override def read(offset: Array[Long], readShape: Array[Int]): ma2.Array =
      arr.read(offset, readShape)
  }

  private final class LegacyArrayReader(arr: LegacyZarrArray) extends ArrayReader {
    override def shape: Array[Int] = arr.metadata.shape.map(_.toInt)
    override def chunkShape: Option[Array[Int]] = Option(arr.metadata.chunkShape()).map(_.map(_.toInt))
    override def read(offset: Array[Long], readShape: Array[Int]): ma2.Array =
      arr.read(offset, readShape)
  }
}
