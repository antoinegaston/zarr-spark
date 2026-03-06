package com.epigene.zarr

class ZarrUtilsSpec extends ZarrBaseSpec {

  test("normalizeStoreRoots handles volumes and local paths") {
    ZarrUtils.normalizeStoreRoots("dbfs:/Volumes/data") shouldBe Seq("dbfs:/Volumes/data")
    ZarrUtils.normalizeStoreRoots("/Volumes/data") shouldBe Seq("dbfs:/Volumes/data", "/Volumes/data")
    ZarrUtils.normalizeStoreRoots("file:/Volumes/data") shouldBe Seq("dbfs:/Volumes/data", "/Volumes/data")
    ZarrUtils.normalizeStoreRoots("/tmp/data") shouldBe Seq("/tmp/data")
    ZarrUtils.normalizeStoreRoots("file:/tmp/data") shouldBe Seq("file:/tmp/data")
  }

  test("normalizeStoreRoots handles empty, dbfs-prefix and workspace paths") {
    ZarrUtils.normalizeStoreRoots("") shouldBe Seq.empty
    ZarrUtils.normalizeStoreRoots("  ") shouldBe Seq.empty
    ZarrUtils.normalizeStoreRoots("/dbfs/Volumes/data") shouldBe Seq("dbfs:/Volumes/data", "/Volumes/data")
    ZarrUtils.normalizeStoreRoots("/Workspace/Volumes/data") shouldBe Seq("dbfs:/Volumes/data", "/Volumes/data")
    ZarrUtils.normalizeStoreRoots("s3a://bucket/path") shouldBe Seq("s3a://bucket/path")
  }

  test("resolveNodePath strips slashes and whitespace") {
    ZarrUtils.resolveNodePath(" /a/b/ ") shouldBe "a/b"
    ZarrUtils.resolveNodePath("/") shouldBe ""
    ZarrUtils.resolveNodePath("") shouldBe ""
    ZarrUtils.resolveNodePath(null) shouldBe ""
  }

  test("boolOpt returns default or parsed value") {
    ZarrUtils.boolOpt(options(), "flag", default = true) shouldBe true
    ZarrUtils.boolOpt(options(), "flag", default = false) shouldBe false
    ZarrUtils.boolOpt(options("flag" -> "true"), "flag", default = false) shouldBe true
    ZarrUtils.boolOpt(options("flag" -> "false"), "flag", default = true) shouldBe false
  }

  test("intOpt returns default or parsed value") {
    ZarrUtils.intOpt(options(), "size", default = 42) shouldBe 42
    ZarrUtils.intOpt(options("size" -> "100"), "size", default = 42) shouldBe 100
  }

  test("strReq throws for missing option") {
    intercept[IllegalArgumentException] {
      ZarrUtils.strReq(options(), "missing")
    }
    ZarrUtils.strReq(options("key" -> "val"), "key") shouldBe "val"
  }

  test("requireColumnsAndIndex throws for missing options") {
    intercept[IllegalArgumentException] {
      ZarrUtils.requireColumnsAndIndex(options())
    }
    intercept[IllegalArgumentException] {
      ZarrUtils.requireColumnsAndIndex(options("columnsNodes" -> "c"))
    }
  }

  test("requireAliases defaults and validates") {
    ZarrUtils.requireAliases(options(), Seq("c"), Seq("i")) shouldBe (Seq("columns"), Seq("indexes"))
    ZarrUtils.requireAliases(
      options("columnAliases" -> "gene", "indexAliases" -> "sample"),
      Seq("c"),
      Seq("i")
    ) shouldBe (Seq("gene"), Seq("sample"))

    ZarrUtils.requireAliases(
      options("columnAliases" -> "gene", "indexAliases" -> "sample"),
      Seq("c1", "c2"),
      Seq("i1", "i2")
    ) shouldBe (Seq("gene_1", "gene_2"), Seq("sample_1", "sample_2"))

    intercept[IllegalArgumentException] {
      ZarrUtils.requireAliases(
        options("columnAliases" -> "dup", "indexAliases" -> "dup"),
        Seq("c"),
        Seq("i")
      )
    }

    intercept[IllegalArgumentException] {
      ZarrUtils.requireAliases(options("columnAliases" -> "value"), Seq("c"), Seq("i"))
    }
  }

  test("requireAliases rejects wrong count") {
    intercept[IllegalArgumentException] {
      ZarrUtils.requireAliases(
        options("columnAliases" -> "a,b,c"),
        Seq("c1", "c2"),
        Seq("i")
      )
    }
  }

  test("requireAliases defaults multi-index without explicit aliases") {
    val (cols, idxs) = ZarrUtils.requireAliases(options(), Seq("c1", "c2"), Seq("i1", "i2"))
    cols shouldBe Seq("columns_1", "columns_2")
    idxs shouldBe Seq("indexes_1", "indexes_2")
  }

  test("layerSpec validates valuesNode") {
    val spec = ZarrUtils.layerSpec(options("valuesNode" -> "layers/raw/"))
    spec match {
      case NodeLayerSpec(nodePath) => nodePath shouldBe "layers/raw/"
    }

    intercept[IllegalArgumentException] {
      ZarrUtils.layerSpec(options("valuesNode" -> "a,b"))
    }

    intercept[IllegalArgumentException] {
      ZarrUtils.layerSpec(options())
    }
  }

  test("requireShape2D accepts 2D and rejects others") {
    ZarrUtils.requireShape2D(Array(3, 5)) shouldBe (3, 5)

    intercept[IllegalArgumentException] {
      ZarrUtils.requireShape2D(Array(2))
    }
    intercept[IllegalArgumentException] {
      ZarrUtils.requireShape2D(Array(2, 3, 4))
    }
  }
}
