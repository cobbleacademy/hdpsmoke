package com.aslick.strm

import org.slf4j.LoggerFactory

/**
  * A Base class to provide protect & unprotect functions using crypto encode decode utilities
  */
object BaseCryptoCodec {
  final val logger = LoggerFactory.getLogger(getClass)

  def createSession(sessUser: String, callUser: String, configFolder: String, callPass: String) = {
    logger.info("createSession() invoked.")
  }

  def protect(fieldValue: String, scheme: String): String = {
    "SZEIG"
  }

  def unprotect(fieldValue: String, scheme: String): String = {
    "FOUND"
  }

}
