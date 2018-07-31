package de.kaufhof.ets.filestorage.core

import java.security.MessageDigest
import java.text.Normalizer
import java.time.{Clock, Instant, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.UUID

import akka.NotUsed
import akka.http.scaladsl.coding.Gzip
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.ByteString
import de.kaufhof.ets.akkastreamutils.core.StreamUtils
import de.kaufhof.ets.akkastreamutils.core.implicits._
import javax.xml.bind.DatatypeConverter

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Failure
import scala.language.higherKinds

case class StoringService(v: String)
case class FileHash(v: String)

case class FileInfo(service: StoringService, fileHash: FileHash, objectPath: String, fileSize: Long, uploadDate: Instant)

trait FutureExecutor[F[_]] {
  def transform[A](fa: F[A]): Future[A] //FunctionK F ~> Future
  def pure[A](value: A): F[A]
  def map[A, B](fa: F[A])(f: A => B): F[B]
}

class UploadRollbackOrCommit[F[_]](undo: () => Future[Unit], _commit: () => F[FileInfo]) {
  //lazy ensures this is only executed once
  lazy val rollback: Future[Unit] = undo()
  lazy val commit: F[FileInfo] = _commit()
}

trait FileStorageService[F[_]] {

  def getFileContent(service: StoringService, fileHash: FileHash): Source[ByteString, NotUsed]
  def getFileContentGzip(service: StoringService, fileHash: FileHash): Source[ByteString, NotUsed]
  def delete(service: StoringService, fileHash: FileHash): Future[Boolean]

  /**
    * Function returns a rollback or commit class.
    * - commit must be called (and executed!) to create an entry for the uploaded object
    * - rollback must be called if the uploaded file should be deleted and no entry will be created
    */
  def uploadWithoutAutoCommit(service: StoringService): Sink[ByteString, Future[UploadRollbackOrCommit[F]]]
}

class FileStorageServiceImpl[F[_]](repo: FileStorageRepository[F],
                                   objectStorage: SwiftObjectStorage,
                                   namingStrategy: (StoringService, Instant) => String = FileStorageService.objectPath)
                                  (implicit ec: ExecutionContext, clock: Clock, executor: FutureExecutor[F])
  extends FileStorageService[F] with ObjectstoreLogging {

  override def getFileContent(service: StoringService, fileHash: FileHash): Source[ByteString, NotUsed] = {
    Source.fromFuture(executor.transform(repo.getFileInfo(service, fileHash)).map{
      case Some(fInfo) => objectStorage.download(fInfo.objectPath)
      case None => Source.failed[ByteString](new Exception("File not found"))
    }).flatMapConcat(identity)
  }

  override def getFileContentGzip(service: StoringService, fileHash: FileHash): Source[ByteString, NotUsed] = {
    Source.fromFuture(executor.transform(repo.getFileInfo(service, fileHash)).map{
      //in case of empty files gzip an empty string otherwise decompressing will throw warnings/errors
      case Some(fInfo) if fInfo.fileSize == 0L => Source.single(ByteString.empty).via(Gzip.encoderFlow)
      case Some(fInfo) => objectStorage.downloadGzip(fInfo.objectPath)
      case None => Source.failed[ByteString](new Exception("File not found"))
    }).flatMapConcat(identity)
  }

  /**
    * Deletes a file with a given hash.
    * ATTENTION: multiple files with same content share the same hash
    */
  override def delete(service: StoringService, fileHash: FileHash): Future[Boolean] =
    for {
      fileInfoOpt <- executor.transform(repo.getFileInfo(service, fileHash))
      _           <- fileInfoOpt.map(fileInfo => objectStorage.delete(fileInfo.objectPath)).getOrElse(Future.successful(()))
      deleted     <- executor.transform(repo.delete(service, fileHash))
    } yield deleted

  override def uploadWithoutAutoCommit(service: StoringService): Sink[ByteString, Future[UploadRollbackOrCommit[F]]] = {

    Flow[ByteString]
      .alsoToMat(FileStorageService.fileDetailSink)(Keep.right)
      .toMat(StreamUtils.fixedBroadcastHub)(Keep.both)
      .mapMaterializedValue{case (fileDetailRes, uploadSource) =>
        val uploadDate = clock.instant()
        val objectPath = namingStrategy(service, uploadDate)

        (for {
          _                    <- objectStorage.upload(objectPath, uploadSource)
          fileDetail           <- fileDetailRes
          existingFileInfoOpt  <- executor.transform(repo.getFileInfo(service, fileDetail._2))
          //check if file really exists in object storage and not only in repository
          fileExists           <- existingFileInfoOpt.map(fi => objectStorage.exists(fi.objectPath)).getOrElse(Future.successful(false))
          //if file exists we don't store the file twice => delete the new uploaded file
          _                    <- if (fileExists) objectStorage.delete(objectPath) else Future.successful(())
        } yield {
          val newFileInfo: FileInfo = FileInfo(service, fileDetail._2, objectPath, fileDetail._1, uploadDate)
          val fileInfo = existingFileInfoOpt.filter(_ => fileExists).getOrElse(newFileInfo)

          def dbUpdate: F[Unit] = if (fileExists) {
            //file exists in object storage and repo
            executor.pure(())
          } else {
            if (existingFileInfoOpt.isDefined) {
              //entry in repo exists
              repo.update(fileInfo.service, fileInfo.fileHash, fileInfo.objectPath)
            } else {
              //entry in repo does not exist
              repo.insert(fileInfo)
            }
          }

          val commit = () => executor.map(dbUpdate)(_ => fileInfo)

          new UploadRollbackOrCommit[F](createUndo(fileExists, objectPath), commit)
        }).andThen{
          case Failure(exc) =>
            //try to delete file if upload fails
            objectStorage.delete(objectPath)
              .recover{ case _: Throwable => log.error("Could not delete file in object storage after prev. failure", exc) }
        }
      }
  }

  private def createUndo(fileExists: Boolean, objectPath: String) = () => {
    if (fileExists) {
      Future.successful(())
    } else {
      objectStorage.delete(objectPath)
        .andThen { case Failure(exc) => log.error("Deleting file in object storage failed when trying to rollback", exc)}
    }
  }

}

private[core] object FileStorageService {

  def objectPath(uploadService: StoringService, uploadDate: Instant): String = {
    val folder = uploadDate.atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_LOCAL_DATE)
    val uuid = UUID.randomUUID().toString
    s"filestorage/${StringUtils.kebabCase(uploadService.v)}/$folder/$uuid.gz"
  }

  def fileDetailSink(implicit ec: ExecutionContext): Sink[ByteString, Future[(Long, FileHash)]] = {

    Flow[ByteString]
      .statefulFold((0L, MessageDigest.getInstance("MD5"))){ (state, bs) =>
        val (size, md) = state
        md.update(bs.asByteBuffer)
        (size + bs.size.toLong, md)
      }
      .toMat(Sink.head)(Keep.right)
      .mapMaterializedValue(_.map { case (size, md) => (size, FileHash(DatatypeConverter.printHexBinary(md.digest()))) })
  }

}

private[core] object StringUtils {
  private val stripAccentsR = """\p{InCombiningDiacriticalMarks}+""".r
  private val kebabCaseR = """[^A-Za-z0-9]+""".r

  def convertUmlaut(s: String): String = {
    s.flatMap{
      case 'ä' => "ae"
      case 'Ä' => "Ae"
      case 'ö' => "oe"
      case 'Ö' => "Oe"
      case 'ü' => "ue"
      case 'Ü' => "Ue"
      case 'ß' => "ss"
      case x => x.toString
    }
  }

  def stripAccents(s: String): String = {
    val nfdString = Normalizer.normalize(s, Normalizer.Form.NFD)
    stripAccentsR.replaceAllIn(nfdString, "")
  }

  def kebabCase(s: String): String = {
    val cleanS = stripAccents(convertUmlaut(s)).toLowerCase
    kebabCaseR.replaceAllIn(cleanS, "-").stripPrefix("-").stripSuffix("-")
  }

}
