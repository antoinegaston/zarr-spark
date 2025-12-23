package com.epigene.zarr

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class ZarrStringArrayV3Spec extends AnyFunSuite with Matchers {

  private def options(pairs: (String, String)*): CaseInsensitiveStringMap =
    new CaseInsensitiveStringMap(pairs.toMap.asJava)

  test("cachedStringArray reads v3 string arrays") {
    assume(TestEnv.hadoopAvailable, "Hadoop UserGroupInformation is not supported on this JDK.")
    ZarrTestUtils.withTempDir() { root =>
      val namesPath = root.resolve("names")
      val expected = Seq("alpha", "beta", "gamma")
      ZarrTestUtils.writeV3StringArray(namesPath, expected)

      val opts = options("path" -> root.toString)
      val out = ZarrUtils.cachedStringArray(opts, new Configuration(), "names")
      out.toSeq shouldBe expected
    }
  }
}
