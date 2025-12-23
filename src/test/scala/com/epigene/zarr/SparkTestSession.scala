package com.epigene.zarr

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SparkTestSession extends BeforeAndAfterAll { self: Suite =>
  @transient protected var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    if (!TestEnv.hadoopAvailable) return
    spark = SparkSession.builder()
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
