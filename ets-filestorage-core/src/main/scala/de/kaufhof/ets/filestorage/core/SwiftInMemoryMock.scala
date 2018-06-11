package de.kaufhof.ets.filestorage.core

import akka.NotUsed
import akka.http.scaladsl.coding.Gzip
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.stm.{Ref, atomic}

class SwiftInMemoryMock(implicit ec: ExecutionContext, mat: Materializer) extends SwiftObjectStorage {

  private val memoryStore = Ref(Map.empty[String, ByteString])
  private val chunkSize = 8192

  override def upload(objectPath: String, src: Source[ByteString, NotUsed]): Future[Unit] =
    src.runWith(Sink.fold(ByteString.empty)(_ ++ _)).map{fileContent =>
      atomic {implicit txn =>
        memoryStore() = memoryStore() + (objectPath -> fileContent)
      }
    }

  override def download(objectPath: String): Source[ByteString, NotUsed] =
    memoryStore.single().get(objectPath) match {
      case Some(fileContent) => Source.unfold(fileContent){fileContent =>
        val chunk = fileContent.take(chunkSize)
        val remaining = fileContent.drop(chunkSize)
        Some((remaining, chunk)).filter(_ => chunk.nonEmpty)
      }
      case None =>
        Source.failed(ObjectNotExistingException(objectPath))
    }

  override def downloadGzip(objectPath: String): Source[ByteString, NotUsed] =
    download(objectPath).via(Gzip.encoderFlow)

  override def delete(objectPath: String): Future[Unit] = Future.successful{atomic{implicit txn =>
    memoryStore() = memoryStore() - objectPath
  }}

  override def exists(objectPath: String): Future[Boolean] = Future.successful(memoryStore.single().contains(objectPath))

  def get(objectPath: String): Option[ByteString] =
    memoryStore.single().get(objectPath)

  def put(objectPath: String, content: ByteString): Unit = atomic{ implicit txn =>
    memoryStore() = memoryStore() + (objectPath -> content)
  }

  def count: Int =
    memoryStore.single().size

}
