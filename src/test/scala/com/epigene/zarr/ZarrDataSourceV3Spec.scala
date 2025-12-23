package com.epigene.zarr

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ZarrDataSourceV3Spec extends AnyFunSuite with Matchers with SparkTestSession {

  test("reads v3 arrays with string names and alias columns") {
    assume(TestEnv.hadoopAvailable, "Hadoop UserGroupInformation is not supported on this JDK.")
    ZarrTestUtils.withTempDir() { root =>
      val valuesPath = root.resolve("values")
      val columnsPath = root.resolve("columns")
      val indexPath = root.resolve("index")

      ZarrTestUtils.writeV3FloatArray(valuesPath, rows = 2, cols = 3, data = Array[Float](1, 2, 3, 4, 5, 6))
      ZarrTestUtils.writeV3StringArray(columnsPath, Seq("g1", "g2", "g3"))
      ZarrTestUtils.writeV3StringArray(indexPath, Seq("s1", "s2"))

      val df = spark.read
        .format("zarr")
        .option("path", root.toString)
        .option("valuesNode", "values")
        .option("columnsNodes", "columns")
        .option("indexNodes", "index")
        .option("columnAliases", "gene")
        .option("indexAliases", "sample")
        .load()

      df.columns shouldBe Array("sample", "gene", "value")

      val rows = df.where("gene = 'g2'")
        .select("sample", "value")
        .collect()
        .map(r => r.getString(0) -> r.getFloat(1))
        .toMap

      rows shouldBe Map("s1" -> 2.0f, "s2" -> 5.0f)
    }
  }

  test("supports multi-index nodes with alias lists") {
    assume(TestEnv.hadoopAvailable, "Hadoop UserGroupInformation is not supported on this JDK.")
    ZarrTestUtils.withTempDir() { root =>
      val valuesPath = root.resolve("values")
      val columnsPath = root.resolve("columns")
      val columnsAltPath = root.resolve("columns_alt")
      val indexPath = root.resolve("index")
      val indexAltPath = root.resolve("index_alt")

      ZarrTestUtils.writeV3FloatArray(valuesPath, rows = 2, cols = 2, data = Array[Float](1, 2, 3, 4))
      ZarrTestUtils.writeV3StringArray(columnsPath, Seq("g1", "g2"))
      ZarrTestUtils.writeV3StringArray(columnsAltPath, Seq("id1", "id2"))
      ZarrTestUtils.writeV3StringArray(indexPath, Seq("s1", "s2"))
      ZarrTestUtils.writeV3StringArray(indexAltPath, Seq("sid1", "sid2"))

      val df = spark.read
        .format("zarr")
        .option("path", root.toString)
        .option("valuesNode", "values")
        .option("columnsNodes", "columns,columns_alt")
        .option("indexNodes", "index,index_alt")
        .option("columnAliases", "gene,gene_id")
        .option("indexAliases", "sample,sample_id")
        .load()

      df.columns shouldBe Array("sample", "sample_id", "gene", "gene_id", "value")

      val rows = df.where("gene = 'g2'")
        .select("sample", "sample_id", "gene_id", "value")
        .collect()
        .map(r => (r.getString(0), r.getString(1), r.getString(2), r.getFloat(3)))
        .toSet

      rows shouldBe Set(("s1", "sid1", "id2", 2.0f), ("s2", "sid2", "id2", 4.0f))
    }
  }
}
