package com.epigene.zarr

class ZarrUtilsSpec extends ZarrBaseSpec {

  test("normalizeStoreRoots handles volumes and local paths") {
    ZarrUtils.normalizeStoreRoots("dbfs:/Volumes/data") shouldBe Seq("dbfs:/Volumes/data")
    ZarrUtils.normalizeStoreRoots("/Volumes/data") shouldBe Seq("dbfs:/Volumes/data", "/Volumes/data")
    ZarrUtils.normalizeStoreRoots("file:/Volumes/data") shouldBe Seq("dbfs:/Volumes/data", "/Volumes/data")
    ZarrUtils.normalizeStoreRoots("/tmp/data") shouldBe Seq("/tmp/data")
    ZarrUtils.normalizeStoreRoots("file:/tmp/data") shouldBe Seq("file:/tmp/data")
  }

  test("resolveNodePath strips slashes and whitespace") {
    ZarrUtils.resolveNodePath(" /a/b/ ") shouldBe "a/b"
    ZarrUtils.resolveNodePath("/") shouldBe ""
    ZarrUtils.resolveNodePath("") shouldBe ""
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

  test("layerSpec validates valuesNode") {
    val spec = ZarrUtils.layerSpec(options("valuesNode" -> "layers/raw/"))
    spec match {
      case NodeLayerSpec(nodePath) => nodePath shouldBe "layers/raw/"
    }

    intercept[IllegalArgumentException] {
      ZarrUtils.layerSpec(options("valuesNode" -> "a,b"))
    }
  }

  test("requireShape2D rejects non-2D shapes") {
    intercept[IllegalArgumentException] {
      ZarrUtils.requireShape2D(Array(2))
    }
    intercept[IllegalArgumentException] {
      ZarrUtils.requireShape2D(Array(2, 3, 4))
    }
  }
}
