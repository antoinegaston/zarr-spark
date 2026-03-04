package com.epigene.zarr

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.mutable.ArrayBuffer

/**
 * Spark format: "zarr"
 *
 * Required options:
 *   path           – Zarr store root (dbfs:/..., s3a://..., abfs://..., gs://..., file:/...)
 *   valuesNode     – array node path relative to the store root
 *   columnsNodes   – 1D column-names array path(s), comma-separated for multi-index
 *   indexNodes     – 1D index/row-names array path(s), comma-separated for multi-index
 *
 * Layouts:
 *   Zarr v3: detected by presence of zarr.json at the store root.
 *   Zarr v2: detected by presence of .zarray at the resolved node path.
 *
 * Chunk/partitioning options:
 *   useChunkPartitioning – true (default): partitions align with Zarr chunks
 *   indexPerChunk        – override index step when not using chunk partitioning
 *   columnsPerBlock      – block width along columns (default from chunk metadata)
 *
 * Output schema (long format, one row per value):
 *   One or more string columns for indexes and columns (names from aliases), plus a `value: Float`.
 *
 * Name lookup options:
 *   columnAliases/indexAliases – optional comma-separated aliases; defaults to `columns`/`indexes`.
 *   When a single alias is provided for multiple nodes, suffixes `_1`, `_2`, ... are appended.
 *
 * Name arrays are cached per JVM for reuse across partitions.
 */
class ZarrDataSource extends TableProvider with DataSourceRegister {
  override def shortName(): String = "zarr"

  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    ZarrSchemas.schemaFor(options)

  override def getTable(
      schema: StructType,
      transforms: Array[Transform],
      properties: java.util.Map[String, String]
  ): Table = {
    val options = new CaseInsensitiveStringMap(properties)
    new ZarrTable(options)
  }
}

object ZarrSchemas {
  def schemaFor(options: CaseInsensitiveStringMap): StructType = {
    ZarrUtils.strReq(options, "valuesNode")
    val (columnsNodes, indexNodes) = ZarrUtils.requireColumnsAndIndex(options)
    val (columnAliases, indexAliases) = ZarrUtils.requireAliases(options, columnsNodes, indexNodes)
    val fields = ArrayBuffer[StructField]()
    indexAliases.foreach(alias => fields += StructField(alias, StringType, nullable = false))
    columnAliases.foreach(alias => fields += StructField(alias, StringType, nullable = false))
    fields += StructField("value", FloatType, nullable = false)
    new StructType(fields.toArray)
  }
}

/** Represents the values array node. */
sealed trait LayerSpec extends Serializable {
  def openArray(options: CaseInsensitiveStringMap, conf: Configuration): (ZarrUtils.ArrayReader, ZarrUtils.ArrayMeta)
}

final case class NodeLayerSpec(nodePath: String) extends LayerSpec {
  override def openArray(
      options: CaseInsensitiveStringMap,
      conf: Configuration
  ): (ZarrUtils.ArrayReader, ZarrUtils.ArrayMeta) =
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
    val lowOk = indexMax.forall(max => start.toLong <= max)
    val highOk = indexMin.forall(min => end >= min)
    lowOk && highOk
  }
}
