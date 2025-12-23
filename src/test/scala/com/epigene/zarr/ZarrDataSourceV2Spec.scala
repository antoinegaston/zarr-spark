package com.epigene.zarr

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ZarrDataSourceV2Spec extends AnyFunSuite with Matchers with SparkTestSession {

  test("reads v2 arrays and maps numeric names to strings") {
    assume(TestEnv.hadoopAvailable, "Hadoop UserGroupInformation is not supported on this JDK.")
    ZarrTestUtils.withTempDir() { root =>
      val valuesPath = root.resolve("values")
      val columnsPath = root.resolve("columns")
      val indexPath = root.resolve("index")

      ZarrTestUtils.writeV2FloatArray(valuesPath, rows = 2, cols = 3, data = Array[Float](1, 2, 3, 4, 5, 6))
      ZarrTestUtils.writeV2IntArray1D(columnsPath, Array(10, 11, 12))
      ZarrTestUtils.writeV2IntArray1D(indexPath, Array(100, 101))

      val df = spark.read
        .format("zarr")
        .option("path", root.toString)
        .option("valuesNode", "values")
        .option("columnsNodes", "columns")
        .option("indexNodes", "index")
        .load()

      df.columns shouldBe Array("indexes", "columns", "value")

      val rows = df.where("columns = '11'")
        .select("indexes", "value")
        .collect()
        .map(r => r.getString(0) -> r.getFloat(1))
        .toMap

      rows shouldBe Map("100" -> 2.0f, "101" -> 5.0f)
    }
  }
}
