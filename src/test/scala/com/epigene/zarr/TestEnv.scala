package com.epigene.zarr

import java.security.AccessController
import javax.security.auth.Subject

object TestEnv {
  lazy val hadoopAvailable: Boolean = {
    try {
      Subject.getSubject(AccessController.getContext())
      true
    } catch {
      case _: UnsupportedOperationException => false
    }
  }
}
