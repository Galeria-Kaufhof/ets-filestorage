package de.kaufhof.ets.filestorage.core

import scala.language.higherKinds

trait FileStorageRepository[F[_]] {

  def getFileInfo(service: StoringService, fileHash: FileHash): F[Option[FileInfo]]
  def save(fileInfo: FileInfo): F[Unit]
  def delete(service: StoringService, fileHash: FileHash): F[Boolean]

}
