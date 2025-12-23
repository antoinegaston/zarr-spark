package com.epigene.zarr

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class ZarrUtilsSpec extends AnyFunSuite with Matchers {

  private def options(pairs: (String, String)*): CaseInsensitiveStringMap =
    new CaseInsensitiveStringMap(pairs.toMap.asJava)

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

  test("axisMapping rejects non-2D shapes") {
    intercept[IllegalArgumentException] {
      ZarrUtils.axisMapping(Array(2))
    }
    intercept[IllegalArgumentException] {
      ZarrUtils.axisMapping(Array(2, 3, 4))
    }
  }
}
