package de.kaufhof.ets.filestorage.core

import scala.language.higherKinds

/**
  * Expected primary key is: (StoringService, FileHash)
  */
trait FileStorageRepository[F[_]] {

  def getFileInfo(service: StoringService, fileHash: FileHash): F[Option[FileInfo]]
  def insert(fileInfo: FileInfo): F[Unit]
  def update(service: StoringService, fileHash: FileHash, objectPath: String): F[Unit]
  def delete(service: StoringService, fileHash: FileHash): F[Boolean]

}
