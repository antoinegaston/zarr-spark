package com.epigene.zarr

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import dev.zarr.zarrjava.v2.{Array => V2Array, DataType => V2DataType}
import dev.zarr.zarrjava.v3.{Array => V3Array, DataType => V3DataType}
import ucar.ma2.{ArrayFloat, ArrayInt}

import scala.jdk.CollectionConverters._

object ZarrTestUtils {
  def withTempDir[T](prefix: String = "zarr-test")(f: Path => T): T = {
    val dir = Files.createTempDirectory(prefix)
    try f(dir)
    finally deleteRecursively(dir)
  }

  def deleteRecursively(path: Path): Unit = {
    if (!Files.exists(path)) return
    if (Files.isDirectory(path)) {
      val stream = Files.list(path)
      try stream.iterator().asScala.foreach(deleteRecursively)
      finally stream.close()
    }
    Files.deleteIfExists(path)
  }

  def writeV3FloatArray(
      path: Path,
      rows: Int,
      cols: Int,
      data: Array[Float],
      chunkRows: Int = -1,
      chunkCols: Int = -1
  ): Unit = {
    val cRows = if (chunkRows <= 0) rows else chunkRows
    val cCols = if (chunkCols <= 0) cols else chunkCols
    val meta = V3Array
      .metadataBuilder()
      .withShape(rows.toLong, cols.toLong)
      .withDataType(V3DataType.FLOAT32)
      .withChunkShape(cRows, cCols)
      .build()
    val arr = V3Array.create(path.toString, meta)
    val ma = new ArrayFloat.D2(rows, cols)
    var idx = 0
    var r = 0
    while (r < rows) {
      var c = 0
      while (c < cols) {
        ma.set(r, c, data(idx))
        idx += 1
        c += 1
      }
      r += 1
    }
    arr.write(ma)
  }

  def writeV2FloatArray(
      path: Path,
      rows: Int,
      cols: Int,
      data: Array[Float],
      chunkRows: Int = -1,
      chunkCols: Int = -1
  ): Unit = {
    val cRows = if (chunkRows <= 0) rows else chunkRows
    val cCols = if (chunkCols <= 0) cols else chunkCols
    val meta = V2Array
      .metadataBuilder()
      .withShape(rows.toLong, cols.toLong)
      .withChunks(cRows, cCols)
      .withDataType(V2DataType.FLOAT32)
      .build()
    val arr = V2Array.create(path.toString, meta)
    val ma = new ArrayFloat.D2(rows, cols)
    var idx = 0
    var r = 0
    while (r < rows) {
      var c = 0
      while (c < cols) {
        ma.set(r, c, data(idx))
        idx += 1
        c += 1
      }
      r += 1
    }
    arr.write(ma)
  }

  def writeV2IntArray1D(path: Path, values: Array[Int], chunkSize: Int = -1): Unit = {
    val cSize = if (chunkSize <= 0) values.length else chunkSize
    val meta = V2Array
      .metadataBuilder()
      .withShape(values.length.toLong)
      .withChunks(cSize)
      .withDataType(V2DataType.INT32)
      .build()
    val arr = V2Array.create(path.toString, meta)
    val ma = new ArrayInt.D1(values.length, false)
    var i = 0
    while (i < values.length) {
      ma.set(i, values(i))
      i += 1
    }
    arr.write(ma)
  }

  def writeV3StringArray(path: Path, values: Seq[String], chunkSize: Int = -1): Unit = {
    val total = values.length
    val cSize = if (chunkSize <= 0) total else chunkSize
    if (total > cSize) {
      throw new IllegalArgumentException("writeV3StringArray only supports a single chunk in tests.")
    }
    Files.createDirectories(path)
    val metaJson =
      s"""{
         |  "zarr_format": 3,
         |  "node_type": "array",
         |  "data_type": "string",
         |  "shape": [$total],
         |  "chunk_grid": { "name": "regular", "configuration": { "chunk_shape": [$cSize] } },
         |  "chunk_key_encoding": { "name": "default", "configuration": { "separator": "/" } },
         |  "codecs": [ { "name": "vlen-utf8" } ],
         |  "fill_value": ""
         |}""".stripMargin
    Files.write(path.resolve("zarr.json"), metaJson.getBytes(StandardCharsets.UTF_8))

    val encoded = encodeVlenUtf8(values)
    val chunkDir = path.resolve("c")
    Files.createDirectories(chunkDir)
    Files.write(chunkDir.resolve("0"), encoded)
  }

  private def encodeVlenUtf8(values: Seq[String]): Array[Byte] = {
    val payloadSizes = values.map(_.getBytes(StandardCharsets.UTF_8))
    val totalLen = 4 + payloadSizes.map(b => 4 + b.length).sum
    val buf = ByteBuffer.allocate(totalLen).order(ByteOrder.LITTLE_ENDIAN)
    buf.putInt(values.length)
    payloadSizes.foreach { bytes =>
      buf.putInt(bytes.length)
      buf.put(bytes)
    }
    buf.array()
  }
}
