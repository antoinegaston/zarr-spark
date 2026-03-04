package com.epigene.zarr

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.sources._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

object ZarrFilters {

  def isSupportedForPruning(f: Filter, options: CaseInsensitiveStringMap): Boolean = {
    val (columnsNodes, indexNodes) = ZarrUtils.requireColumnsAndIndex(options)
    val (columnAttrs, indexAttrs) = ZarrUtils.requireAliases(options, columnsNodes, indexNodes)
    f match {
      case EqualTo(attr, _) if supportedAttr(attr, indexAttrs, columnAttrs) => true
      case In(attr, _) if supportedAttr(attr, indexAttrs, columnAttrs)      => true
      case _                                                                => false
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
    val (columnsNodes, indexNodes) = ZarrUtils.requireColumnsAndIndex(options)
    val (columnAliases, indexAliases) = ZarrUtils.requireAliases(options, columnsNodes, indexNodes)
    val columnAliasToNode = columnAliases.zip(columnsNodes).toMap
    val indexAliasToNode = indexAliases.zip(indexNodes).toMap

    def tightenMin(v: Long): Unit = min = Some(min.fold(v)(m => Math.max(m, v)))
    def tightenMax(v: Long): Unit = max = Some(max.fold(v)(m => Math.min(m, v)))
    def updateNames(
        target: scala.collection.mutable.Map[String, Set[String]],
        alias: String,
        incoming: Set[String]
    ): Unit = {
      val next = target.get(alias).fold(incoming)(_ intersect incoming)
      target.update(alias, next)
    }

    filters.foreach {
      case EqualTo(attr, v) if indexAliasToNode.contains(attr) =>
        updateNames(indexNamesByAlias, attr, Set(toStr(v)))

      case In(attr, vs) if indexAliasToNode.contains(attr) =>
        updateNames(indexNamesByAlias, attr, vs.map(toStr).toSet)

      case EqualTo(attr, v) if columnAliasToNode.contains(attr) =>
        updateNames(columnNamesByAlias, attr, Set(toStr(v)))

      case In(attr, vs) if columnAliasToNode.contains(attr) =>
        updateNames(columnNamesByAlias, attr, vs.map(toStr).toSet)

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
    case other     => other.toString
  }
}
