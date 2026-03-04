package com.epigene.zarr

import org.apache.hadoop.conf.Configuration

class ZarrStringArraySpec extends ZarrBaseSpec {

  test("cachedStringArray reads v3 string arrays") {
    assumeHadoop()
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
