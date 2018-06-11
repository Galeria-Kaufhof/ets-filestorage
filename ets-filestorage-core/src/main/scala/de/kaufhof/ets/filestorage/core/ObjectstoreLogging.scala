package de.kaufhof.ets.filestorage.core

import org.slf4j.LoggerFactory

/**
  * A specialization of `StructuredModuleLogging` for object store
  */
trait ObjectstoreLogging {
  val log = LoggerFactory.getLogger(getClass)
}

