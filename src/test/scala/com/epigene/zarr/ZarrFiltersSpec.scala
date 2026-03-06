package com.epigene.zarr

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.sources.{EqualTo, GreaterThan, In}

class ZarrFiltersSpec extends ZarrBaseSpec {

  private val filterOpts = options(
    "columnsNodes" -> "columns",
    "indexNodes" -> "index",
    "columnAliases" -> "gene",
    "indexAliases" -> "sample"
  )

  test("isSupportedForPruning accepts EqualTo on aliases") {
    ZarrFilters.isSupportedForPruning(EqualTo("gene", "TP53"), filterOpts) shouldBe true
    ZarrFilters.isSupportedForPruning(EqualTo("sample", "s1"), filterOpts) shouldBe true
    ZarrFilters.isSupportedForPruning(EqualTo("column", "TP53"), filterOpts) shouldBe false
  }

  test("isSupportedForPruning accepts In on aliases") {
    ZarrFilters.isSupportedForPruning(In("gene", Array("TP53", "BRCA1")), filterOpts) shouldBe true
    ZarrFilters.isSupportedForPruning(In("sample", Array("s1")), filterOpts) shouldBe true
    ZarrFilters.isSupportedForPruning(In("unknown", Array("x")), filterOpts) shouldBe false
  }

  test("isSupportedForPruning rejects unsupported filter types") {
    ZarrFilters.isSupportedForPruning(GreaterThan("gene", "A"), filterOpts) shouldBe false
  }

  // --- PruneSpec direct unit tests ---

  test("PruneSpec.overlapsIndex with no constraints allows everything") {
    val spec = PruneSpec()
    spec.overlapsIndex(0, 10) shouldBe true
    spec.overlapsIndex(100, 1) shouldBe true
  }

  test("PruneSpec.overlapsIndex with min and max constraints") {
    val spec = PruneSpec(indexMin = Some(5L), indexMax = Some(10L))
    spec.overlapsIndex(0, 3) shouldBe false   // range [0,2] < min 5
    spec.overlapsIndex(0, 6) shouldBe true    // range [0,5] overlaps [5,10]
    spec.overlapsIndex(5, 3) shouldBe true    // range [5,7] inside [5,10]
    spec.overlapsIndex(10, 1) shouldBe true   // range [10,10] at max
    spec.overlapsIndex(11, 5) shouldBe false  // range [11,15] > max 10
  }

  test("PruneSpec.overlapsIndex with empty range (min > max) rejects everything") {
    val spec = PruneSpec(indexMin = Some(Long.MaxValue), indexMax = Some(Long.MinValue))
    spec.overlapsIndex(0, 1000) shouldBe false
  }

  test("PruneSpec.allowsColumnBlockStart with no constraints allows everything") {
    val spec = PruneSpec()
    spec.allowsColumnBlockStart(0) shouldBe true
    spec.allowsColumnBlockStart(1024) shouldBe true
  }

  test("PruneSpec.allowsColumnBlockStart with constraints") {
    val spec = PruneSpec(columnBlockStarts = Some(Set(0, 100)))
    spec.allowsColumnBlockStart(0) shouldBe true
    spec.allowsColumnBlockStart(100) shouldBe true
    spec.allowsColumnBlockStart(50) shouldBe false
  }

  test("PruneSpec.allowsColumnBlockStart with empty set rejects everything") {
    val spec = PruneSpec(columnBlockStarts = Some(Set.empty))
    spec.allowsColumnBlockStart(0) shouldBe false
  }

  // --- toPruneSpec integration tests ---

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

  test("toPruneSpec with no filters returns unconstrained spec") {
    assumeHadoop()
    ZarrTestUtils.withTempDir() { root =>
      ZarrTestUtils.writeV3StringArray(root.resolve("columns"), Seq("g0"))
      ZarrTestUtils.writeV3StringArray(root.resolve("index"), Seq("s0"))

      val opts = options(
        "path" -> root.toString,
        "columnsNodes" -> "columns",
        "indexNodes" -> "index"
      )

      val spec = ZarrFilters.toPruneSpec(Array.empty, opts, new Configuration(), columnsPerBlock = 1)
      spec.indexMin shouldBe None
      spec.indexMax shouldBe None
      spec.columnBlockStarts shouldBe None
    }
  }

  test("toPruneSpec with non-existent name produces empty range") {
    assumeHadoop()
    ZarrTestUtils.withTempDir() { root =>
      ZarrTestUtils.writeV3StringArray(root.resolve("columns"), Seq("g0", "g1"))
      ZarrTestUtils.writeV3StringArray(root.resolve("index"), Seq("s0", "s1"))

      val opts = options(
        "path" -> root.toString,
        "columnsNodes" -> "columns",
        "indexNodes" -> "index",
        "columnAliases" -> "gene",
        "indexAliases" -> "sample"
      )

      val spec = ZarrFilters.toPruneSpec(
        Array(EqualTo("gene", "MISSING")),
        opts,
        new Configuration(),
        columnsPerBlock = 2
      )
      spec.columnBlockStarts shouldBe Some(Set.empty)

      val spec2 = ZarrFilters.toPruneSpec(
        Array(EqualTo("sample", "MISSING")),
        opts,
        new Configuration(),
        columnsPerBlock = 2
      )
      spec2.indexMin shouldBe Some(Long.MaxValue)
      spec2.indexMax shouldBe Some(Long.MinValue)
    }
  }

  test("toPruneSpec with integer filter values uses toString") {
    assumeHadoop()
    ZarrTestUtils.withTempDir() { root =>
      ZarrTestUtils.writeV3StringArray(root.resolve("columns"), Seq("0", "1", "2"))
      ZarrTestUtils.writeV3StringArray(root.resolve("index"), Seq("100", "101"))

      val opts = options(
        "path" -> root.toString,
        "columnsNodes" -> "columns",
        "indexNodes" -> "index",
        "columnAliases" -> "gene",
        "indexAliases" -> "sample"
      )

      val spec = ZarrFilters.toPruneSpec(
        Array(EqualTo("sample", 101: java.lang.Integer)),
        opts,
        new Configuration(),
        columnsPerBlock = 2
      )
      spec.indexMin shouldBe Some(1L)
      spec.indexMax shouldBe Some(1L)
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
