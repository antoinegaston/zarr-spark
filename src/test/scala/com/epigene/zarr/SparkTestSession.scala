package com.epigene.zarr

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll

trait SparkTestSession extends BeforeAndAfterAll { self: ZarrBaseSpec =>
  @transient protected var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    if (!hadoopAvailable) return
    spark = SparkSession
      .builder()
      .appName("zarr-test")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
    }
    super.afterAll()
  }
}
