package com.epigene.zarr

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.sources.{EqualTo, In}

class ZarrFiltersSpec extends ZarrBaseSpec {

  test("isSupportedForPruning respects aliases") {
    val opts = options(
      "columnsNodes" -> "columns",
      "indexNodes" -> "index",
      "columnAliases" -> "gene",
      "indexAliases" -> "sample"
    )
    ZarrFilters.isSupportedForPruning(EqualTo("gene", "TP53"), opts) shouldBe true
    ZarrFilters.isSupportedForPruning(EqualTo("sample", "s1"), opts) shouldBe true
    ZarrFilters.isSupportedForPruning(EqualTo("column", "TP53"), opts) shouldBe false
  }

  test("toPruneSpec maps alias filters to index ranges and column blocks") {
    assumeHadoop()
    ZarrTestUtils.withTempDir() { root =>
      val columnsPath = root.resolve("columns")
      val indexPath = root.resolve("index")

      ZarrTestUtils.writeV3StringArray(columnsPath, Seq("g0", "g1", "g2", "g3"))
      ZarrTestUtils.writeV3StringArray(indexPath, Seq("s0", "s1", "s2"))

      val opts = options(
        "path" -> root.toString,
        "columnsNodes" -> "columns",
        "indexNodes" -> "index",
        "columnAliases" -> "gene",
        "indexAliases" -> "sample"
      )

      val filters: Array[org.apache.spark.sql.sources.Filter] = Array(
        In("gene", Array[Any]("g1", "g2")),
        EqualTo("sample", "s1")
      )

      val spec = ZarrFilters.toPruneSpec(filters, opts, new Configuration(), columnsPerBlock = 2)
      spec.indexMin shouldBe Some(1L)
      spec.indexMax shouldBe Some(1L)
      spec.columnBlockStarts.getOrElse(Set.empty) shouldBe Set(0, 2)
    }
  }

  test("toPruneSpec respects multi-index aliases") {
    assumeHadoop()
    ZarrTestUtils.withTempDir() { root =>
      val columnsPath = root.resolve("columns")
      val columnsAltPath = root.resolve("columns_alt")
      val indexPath = root.resolve("index")
      val indexAltPath = root.resolve("index_alt")

      ZarrTestUtils.writeV3StringArray(columnsPath, Seq("g0", "g1", "g2"))
      ZarrTestUtils.writeV3StringArray(columnsAltPath, Seq("id0", "id1", "id2"))
      ZarrTestUtils.writeV3StringArray(indexPath, Seq("s0", "s1", "s2"))
      ZarrTestUtils.writeV3StringArray(indexAltPath, Seq("sid0", "sid1", "sid2"))

      val opts = options(
        "path" -> root.toString,
        "columnsNodes" -> "columns,columns_alt",
        "indexNodes" -> "index,index_alt",
        "columnAliases" -> "gene,gene_id",
        "indexAliases" -> "sample,sample_id"
      )

      val filters: Array[org.apache.spark.sql.sources.Filter] = Array(
        EqualTo("sample_id", "sid2"),
        In("gene_id", Array[Any]("id2"))
      )

      val spec = ZarrFilters.toPruneSpec(filters, opts, new Configuration(), columnsPerBlock = 2)
      spec.indexMin shouldBe Some(2L)
      spec.indexMax shouldBe Some(2L)
      spec.columnBlockStarts.getOrElse(Set.empty) shouldBe Set(2)
    }
  }
}
