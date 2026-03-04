package com.epigene.zarr

import java.io.ByteArrayOutputStream
import java.util
import java.util.stream.{Stream => JStream}
import java.nio.ByteBuffer

import scala.util.Using

import dev.zarr.zarrjava.store.{Store, StoreHandle}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

/**
 * Zarr Store backed by Hadoop FileSystem:
 * dbfs:/, s3a://, abfs://, gs://, file:/ etc.
 *
 * Read-only in this implementation.
 */
final class HadoopZarrStore(rootUri: String, hadoopConf: Configuration) extends Store.ListableStore {

  private val rootPath = new Path(rootUri)
  private val fs: FileSystem = rootPath.getFileSystem(hadoopConf)

  private def resolvePath(key: String): Path = {
    val cleaned = key.stripPrefix("/")
    if (cleaned.isEmpty) rootPath else new Path(rootPath, cleaned)
  }

  private def resolvePath(keys: Array[String]): Path =
    resolvePath(keys.mkString("/"))

  override def get(keys: Array[String]): ByteBuffer =
    ByteBuffer.wrap(readAll(keys))

  override def get(keys: Array[String], start: Long): ByteBuffer = {
    val bytes = readAll(keys)
    if (start >= bytes.length) ByteBuffer.allocate(0)
    else ByteBuffer.wrap(bytes, start.toInt, bytes.length - start.toInt)
  }

  override def get(keys: Array[String], start: Long, end: Long): ByteBuffer = {
    val bytes = readAll(keys)
    val s = Math.max(0L, start).toInt
    val e = Math.min(bytes.length.toLong, end).toInt
    if (s >= e) ByteBuffer.allocate(0) else ByteBuffer.wrap(bytes, s, e - s)
  }

  override def exists(keys: Array[String]): Boolean = fs.exists(resolvePath(keys))

  override def list(prefixKeys: Array[String]): JStream[String] = {
    val prefix = prefixKeys.mkString("/")
    val start = resolvePath(prefix)
    val out = new util.ArrayList[String]()
    if (!fs.exists(start)) return out.stream()

    val status = fs.getFileStatus(start)
    if (status.isFile) {
      out.add(prefix.stripPrefix("/"))
      return out.stream()
    }

    val rootPathStr = Option(rootPath.toUri.getPath).getOrElse(rootPath.toString).stripSuffix("/")

    def keyFor(fullPath: Path): String = {
      val fullPathStr = Option(fullPath.toUri.getPath).getOrElse(fullPath.toString)
      fullPathStr.stripPrefix(rootPathStr).stripPrefix("/")
    }

    def walk(dir: Path): Unit = {
      val stats: Array[FileStatus] = fs.listStatus(dir)
      stats.foreach { st =>
        if (st.isDirectory) walk(st.getPath)
        else {
          out.add(keyFor(st.getPath))
        }
      }
    }

    walk(start)
    out.stream()
  }

  override def set(keys: Array[String], value: ByteBuffer): Unit =
    throw new UnsupportedOperationException("HadoopZarrStore is read-only in this implementation")

  override def delete(keys: Array[String]): Unit =
    throw new UnsupportedOperationException("HadoopZarrStore is read-only in this implementation")

  override def resolve(keys: String*): StoreHandle =
    new StoreHandle(this, keys: _*)

  private def readAll(keys: Array[String]): Array[Byte] = {
    Using.resource(fs.open(resolvePath(keys))) { in =>
      val baos = new ByteArrayOutputStream()
      val buf = new Array[Byte](1024 * 1024)
      var n = in.read(buf)
      while (n >= 0) {
        if (n > 0) baos.write(buf, 0, n)
        n = in.read(buf)
      }
      baos.toByteArray
    }
  }
}
