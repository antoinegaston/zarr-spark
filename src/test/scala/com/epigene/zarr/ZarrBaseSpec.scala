package com.epigene.zarr

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.security.AccessController
import javax.security.auth.Subject

import scala.jdk.CollectionConverters._

/**
 * Base trait for all zarr-spark tests.
 * Provides common helpers for options construction and Hadoop availability checks.
 */
trait ZarrBaseSpec extends AnyFunSuite with Matchers {

  protected lazy val hadoopAvailable: Boolean = {
    try {
      Subject.getSubject(AccessController.getContext())
      true
    } catch {
      case _: UnsupportedOperationException => false
    }
  }

  protected def assumeHadoop(): Unit =
    assume(hadoopAvailable, "Hadoop UserGroupInformation is not supported on this JDK.")

  protected def options(pairs: (String, String)*): CaseInsensitiveStringMap =
    new CaseInsensitiveStringMap(pairs.toMap.asJava)
}
