package com.epigene.zarr

import org.apache.hadoop.conf.Configuration

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import scala.jdk.CollectionConverters._

class HadoopZarrStoreSpec extends ZarrBaseSpec {

  test("reads and lists files from local filesystem") {
    assumeHadoop()
    ZarrTestUtils.withTempDir() { root =>
      val fileA = root.resolve("a").resolve("b.txt")
      val fileB = root.resolve("a").resolve("c.txt")
      Files.createDirectories(fileA.getParent)
      Files.write(fileA, "hello".getBytes(StandardCharsets.UTF_8))
      Files.write(fileB, "world".getBytes(StandardCharsets.UTF_8))

      val store = new HadoopZarrStore(root.toString, new Configuration())
      store.exists(Array("a", "b.txt")) shouldBe true
      store.exists(Array("a", "missing.txt")) shouldBe false

      val buf = store.get(Array("a", "b.txt"))
      val bytes = new Array[Byte](buf.remaining())
      buf.get(bytes)
      new String(bytes, StandardCharsets.UTF_8) shouldBe "hello"

      val listed = store.list(Array("a")).iterator().asScala.toSet
      listed shouldBe Set("a/b.txt", "a/c.txt")
    }
  }
}
