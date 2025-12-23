package com.epigene.zarr

import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class V2ZarrAdapterSpec extends AnyFunSuite with Matchers {

  test("V2ZarrAdapter reads a v2 array written with zarr-java") {
    assume(TestEnv.hadoopAvailable, "Hadoop UserGroupInformation is not supported on this JDK.")
    ZarrTestUtils.withTempDir() { root =>
      val valuesPath = root.resolve("values")
      val data = Array[Float](1f, 2f, 3f, 4f)
      ZarrTestUtils.writeV2FloatArray(valuesPath, rows = 2, cols = 2, data = data)

      val (reader, meta) = V2ZarrAdapter.open(root.toString, "values", new Configuration())
      meta.shape shouldBe Array(2, 2)

      val arr = reader.read(Array(0L, 0L), Array(2, 2))
      val it = arr.getIndexIterator
      val out = scala.collection.mutable.ArrayBuffer.empty[Float]
      while (it.hasNext) out += it.getFloatNext

      out.toArray shouldBe data
    }
  }
}
