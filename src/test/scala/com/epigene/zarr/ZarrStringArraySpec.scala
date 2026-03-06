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

  test("cachedStringArray reads v2 vlen-utf8 (object dtype) string arrays") {
    assumeHadoop()
    ZarrTestUtils.withTempDir() { root =>
      val namesPath = root.resolve("labels")
      val expected = Seq("cell_1", "cell_2", "cell_3", "cell_4")
      ZarrTestUtils.writeV2VLenStringArray(namesPath, expected)

      val opts = options("path" -> root.toString)
      val out = ZarrUtils.cachedStringArray(opts, new Configuration(), "labels")
      out.toSeq shouldBe expected
    }
  }

  test("cachedStringArray reads v2 vlen-utf8 with gzip compression") {
    assumeHadoop()
    ZarrTestUtils.withTempDir() { root =>
      val namesPath = root.resolve("labels")
      val expected = Seq("compressed_a", "compressed_b", "compressed_c")
      ZarrTestUtils.writeV2VLenStringArrayGzip(namesPath, expected)

      val opts = options("path" -> root.toString)
      val out = ZarrUtils.cachedStringArray(opts, new Configuration(), "labels")
      out.toSeq shouldBe expected
    }
  }

  test("cachedStringArray reads v2 fixed-width Unicode (<U) string arrays") {
    assumeHadoop()
    ZarrTestUtils.withTempDir() { root =>
      val namesPath = root.resolve("genes")
      val expected = Seq("TP53", "BRCA1", "EGFR")
      ZarrTestUtils.writeV2FixedUnicodeStringArray(namesPath, expected)

      val opts = options("path" -> root.toString)
      val out = ZarrUtils.cachedStringArray(opts, new Configuration(), "genes")
      out.toSeq shouldBe expected
    }
  }

  test("cachedStringArray reads v2 fixed-width bytes (|S) string arrays") {
    assumeHadoop()
    ZarrTestUtils.withTempDir() { root =>
      val namesPath = root.resolve("ids")
      val expected = Seq("s1", "s2", "s3")
      ZarrTestUtils.writeV2FixedBytesStringArray(namesPath, expected)

      val opts = options("path" -> root.toString)
      val out = ZarrUtils.cachedStringArray(opts, new Configuration(), "ids")
      out.toSeq shouldBe expected
    }
  }

  test("cachedNameIndex builds name-to-position lookup") {
    assumeHadoop()
    ZarrTestUtils.withTempDir() { root =>
      val namesPath = root.resolve("names")
      ZarrTestUtils.writeV3StringArray(namesPath, Seq("a", "b", "c"))

      val opts = options("path" -> root.toString)
      val idx = ZarrUtils.cachedNameIndex(opts, new Configuration(), "names")
      idx shouldBe Map("a" -> 0, "b" -> 1, "c" -> 2)
    }
  }
}
