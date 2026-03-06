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

  test("get with start offset returns suffix of file") {
    assumeHadoop()
    ZarrTestUtils.withTempDir() { root =>
      val file = root.resolve("data.bin")
      Files.write(file, "ABCDEFGH".getBytes(StandardCharsets.UTF_8))

      val store = new HadoopZarrStore(root.toString, new Configuration())

      val buf = store.get(Array("data.bin"), 3)
      val bytes = new Array[Byte](buf.remaining())
      buf.get(bytes)
      new String(bytes, StandardCharsets.UTF_8) shouldBe "DEFGH"
    }
  }

  test("get with start at or beyond length returns empty buffer") {
    assumeHadoop()
    ZarrTestUtils.withTempDir() { root =>
      val file = root.resolve("data.bin")
      Files.write(file, "AB".getBytes(StandardCharsets.UTF_8))

      val store = new HadoopZarrStore(root.toString, new Configuration())
      store.get(Array("data.bin"), 2).remaining() shouldBe 0
      store.get(Array("data.bin"), 100).remaining() shouldBe 0
    }
  }

  test("get with start and end returns byte range") {
    assumeHadoop()
    ZarrTestUtils.withTempDir() { root =>
      val file = root.resolve("data.bin")
      Files.write(file, "ABCDEFGH".getBytes(StandardCharsets.UTF_8))

      val store = new HadoopZarrStore(root.toString, new Configuration())

      val buf = store.get(Array("data.bin"), 2, 5)
      val bytes = new Array[Byte](buf.remaining())
      buf.get(bytes)
      new String(bytes, StandardCharsets.UTF_8) shouldBe "CDE"
    }
  }

  test("get with start >= end returns empty buffer") {
    assumeHadoop()
    ZarrTestUtils.withTempDir() { root =>
      val file = root.resolve("data.bin")
      Files.write(file, "ABCDEFGH".getBytes(StandardCharsets.UTF_8))

      val store = new HadoopZarrStore(root.toString, new Configuration())
      store.get(Array("data.bin"), 5, 5).remaining() shouldBe 0
      store.get(Array("data.bin"), 5, 3).remaining() shouldBe 0
    }
  }

  test("get with end beyond length clamps to file length") {
    assumeHadoop()
    ZarrTestUtils.withTempDir() { root =>
      val file = root.resolve("data.bin")
      Files.write(file, "ABCD".getBytes(StandardCharsets.UTF_8))

      val store = new HadoopZarrStore(root.toString, new Configuration())
      val buf = store.get(Array("data.bin"), 2, 100)
      val bytes = new Array[Byte](buf.remaining())
      buf.get(bytes)
      new String(bytes, StandardCharsets.UTF_8) shouldBe "CD"
    }
  }

  test("set and delete throw UnsupportedOperationException") {
    assumeHadoop()
    ZarrTestUtils.withTempDir() { root =>
      val store = new HadoopZarrStore(root.toString, new Configuration())
      intercept[UnsupportedOperationException] {
        store.set(Array("x"), java.nio.ByteBuffer.allocate(1))
      }
      intercept[UnsupportedOperationException] {
        store.delete(Array("x"))
      }
    }
  }

  test("list on non-existent path returns empty stream") {
    assumeHadoop()
    ZarrTestUtils.withTempDir() { root =>
      val store = new HadoopZarrStore(root.toString, new Configuration())
      store.list(Array("missing")).iterator().asScala.toSeq shouldBe empty
    }
  }

  test("list on a file returns that file as a single entry") {
    assumeHadoop()
    ZarrTestUtils.withTempDir() { root =>
      val file = root.resolve("single.txt")
      Files.write(file, "content".getBytes(StandardCharsets.UTF_8))

      val store = new HadoopZarrStore(root.toString, new Configuration())
      val listed = store.list(Array("single.txt")).iterator().asScala.toSeq
      listed shouldBe Seq("single.txt")
    }
  }

  test("resolve returns a StoreHandle") {
    assumeHadoop()
    ZarrTestUtils.withTempDir() { root =>
      val store = new HadoopZarrStore(root.toString, new Configuration())
      val handle = store.resolve("a", "b")
      handle should not be null
    }
  }
}
