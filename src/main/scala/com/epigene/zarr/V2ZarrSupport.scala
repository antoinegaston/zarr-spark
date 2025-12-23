package com.epigene.zarr

import dev.zarr.zarrjava.store.{Store, StoreHandle}
import dev.zarr.zarrjava.v2.{Array => V2ZarrArray}
import org.apache.hadoop.conf.Configuration
import ucar.ma2

object V2ZarrAdapter {
  import ZarrUtils.{ArrayMeta, ArrayReader}

  def open(baseRoot: String, relativeNode: String, conf: Configuration): (ArrayReader, ArrayMeta) = {
    val cleanNode = Option(relativeNode).getOrElse("").trim.stripPrefix("/")
    val store: Store = new HadoopZarrStore(baseRoot, conf)
    val storeHandle = new StoreHandle(store)
    val keys = cleanNode.split("/").map(_.trim).filter(_.nonEmpty)
    val targetHandle = if (keys.isEmpty) storeHandle else storeHandle.resolve(keys: _*)
    val arr = V2ZarrArray.open(targetHandle)
    val shape = arr.metadata.shape.map(_.toInt)
    val chunks = Option(arr.metadata.chunkShape()).map(_.map(_.toInt))
    val reader = new V2ArrayReader(arr)
    (reader, ArrayMeta(shape, chunks))
  }

  private final class V2ArrayReader(arr: V2ZarrArray) extends ArrayReader {
    override def shape: Array[Int] = arr.metadata.shape.map(_.toInt)
    override def chunkShape: Option[Array[Int]] = Option(arr.metadata.chunkShape()).map(_.map(_.toInt))

    override def read(offset: Array[Long], readShape: Array[Int]): ma2.Array =
      arr.read(offset, readShape)
  }
}
