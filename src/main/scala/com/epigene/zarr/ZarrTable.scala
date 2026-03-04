package com.epigene.zarr

import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.unsafe.types.UTF8String

import scala.jdk.CollectionConverters._

import ucar.ma2

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

  override def pruneColumns(requiredSchema: StructType): Unit =
    this.requiredSchema = requiredSchema

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    filtersForPruning = filters
    pushed = Array.empty
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

    ZarrUtils.requireColumnsAndIndex(options)

    val layer = ZarrUtils.layerSpec(options)
    val (_, valueMeta) = layer.openArray(options, conf)
    val shape = valueMeta.shape
    val (indexSize, columnsSize) = ZarrUtils.requireShape2D(shape)

    val useChunk = ZarrUtils.boolOpt(options, "useChunkPartitioning", default = true)
    val (chunkIndex, chunkColumns) = valueMeta.chunkShape match {
      case Some(ch) if ch.length == 2 => (ch(0), ch(1))
      case _                          => (4096, 1024)
    }

    val indexPerChunk = ZarrUtils.intOpt(options, "indexPerChunk", default = chunkIndex)
    val columnsPerBlock = ZarrUtils.intOpt(options, "columnsPerBlock", default = chunkColumns)
    val indexStep = if (useChunk) chunkIndex else indexPerChunk

    pruneSpec = ZarrFilters.toPruneSpec(filtersForPruning, options, conf, columnsPerBlock)

    val parts = for {
      indexStart <- 0 until indexSize by indexStep
      columnStart <- 0 until columnsSize by columnsPerBlock
      indexLen = Math.min(indexStep, indexSize - indexStart)
      columnLen = Math.min(columnsPerBlock, columnsSize - columnStart)
      if pruneSpec.overlapsIndex(indexStart, indexLen)
      if pruneSpec.allowsColumnBlockStart(columnStart)
    } yield ZarrInputPartition(
      options = optionsMap,
      layerSpec = layer,
      indexStart = indexStart,
      indexLen = indexLen,
      columnStart = columnStart,
      columnLen = columnLen,
      requiredFields = requiredSchema.fieldNames
    )

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

final class ZarrPartitionReader(partition: ZarrInputPartition) extends PartitionReader[InternalRow] {

  private val ciOptions = new CaseInsensitiveStringMap(partition.options.asJava)
  private val conf = ZarrUtils.hadoopConf(ciOptions)
  private val (reader, meta) = partition.layerSpec.openArray(ciOptions, conf)

  private val shape = meta.shape
  private val (indexSize, columnsSize) = ZarrUtils.requireShape2D(shape)

  private val offset: Array[Long] = Array(partition.indexStart.toLong, partition.columnStart.toLong)
  private val readShape: Array[Int] = Array(partition.indexLen, partition.columnLen)

  private val slab: ma2.Array = reader.read(offset, readShape)
  private val it = slab.getIndexIterator

  private val requiredFields = partition.requiredFields
  private val requiredFieldSet = requiredFields.toSet
  private val (columnsNodes, indexNodes) = ZarrUtils.requireColumnsAndIndex(ciOptions)
  private val (columnAliases, indexAliases) = ZarrUtils.requireAliases(ciOptions, columnsNodes, indexNodes)

  private val columnAliasToNames: Map[String, Array[String]] =
    loadNameArrays(columnsNodes, columnAliases, columnsSize, axisLabel = "columnsNodes")
  private val indexAliasToNames: Map[String, Array[String]] =
    loadNameArrays(indexNodes, indexAliases, indexSize, axisLabel = "indexNodes")

  private var localRow = 0
  private var localColumn = 0

  override def next(): Boolean = localRow < partition.indexLen

  override def get(): InternalRow = {
    val value = it.getFloatNext
    val indexValue = partition.indexStart.toLong + localRow.toLong
    val columnIndex = partition.columnStart + localColumn

    val row = new GenericInternalRow(requiredFields.length)
    var i = 0
    while (i < requiredFields.length) {
      requiredFields(i) match {
        case "value" => row.setFloat(i, value)
        case name =>
          indexAliasToNames.get(name) match {
            case Some(names) =>
              val v = if (names == null) null else names(indexValue.toInt)
              row.update(i, if (v == null) null else UTF8String.fromString(v))
            case None =>
              columnAliasToNames.get(name) match {
                case Some(names) =>
                  val v = if (names == null) null else names(columnIndex)
                  row.update(i, if (v == null) null else UTF8String.fromString(v))
                case None => row.update(i, null)
              }
          }
      }
      i += 1
    }

    localColumn += 1
    if (localColumn >= partition.columnLen) {
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
        val names = ZarrUtils.cachedStringArray(ciOptions, conf, node)
        if (names.length < axisSize) {
          throw new IllegalArgumentException(
            s"$axisLabel '$node' length ${names.length} is less than axis dimension $axisSize."
          )
        }
        builder += alias -> names
      }
    }
    builder.result()
  }

  override def close(): Unit = ()
}
