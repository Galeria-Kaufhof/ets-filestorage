package de.kaufhof.ets.filestorage.core


import java.time.{Clock, Instant}

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.util.ByteString

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}
import scala.language.reflectiveCalls

class FileStorageServiceImplTest extends WordSpec with Matchers with MockFactory with BeforeAndAfterAll {

  implicit val as = ActorSystem("FileStorageServiceImplTest")
  import as.dispatcher
  implicit val mat = ActorMaterializer()

  val storingService = StoringService("testservice")
  val testData = (1 to 10).map(x => ByteString(x.toString))
  val fullTestData = testData.reduceLeft(_ ++ _)

  def await[T](a: Future[T]): T = Await.result(a, 5.seconds)

  "FileStorageServiceImpl" should {

    "upload a file when no db entry and file exists" in {
      val f = fixture
      f.repo.getFileInfo _ when (*, *) returns None
      f.repo.save _ when (*) returns (())

      val fileInfo = await(f.runUpload().map(_.commit))

      f.objectStorage.get(fileInfo.objectPath) shouldEqual Some(fullTestData)
      f.objectStorage.count shouldEqual 1

      f.repo.save _ verify fileInfo
    }

    "upload a file when a db entry but no file exists" in {
      val f = fixture
      val prevFileInfo = FileInfo(storingService, FileHash("dummy"), "dummy", 100, Instant.now())

      f.repo.getFileInfo _ when (*, *) returns Some(prevFileInfo)
      f.repo.save _ when (*) returns (())

      val fileInfo = await(f.runUpload().map(_.commit))

      fileInfo.objectPath shouldNot be(prevFileInfo.objectPath)

      f.objectStorage.get(fileInfo.objectPath) shouldEqual Some(fullTestData)
      f.objectStorage.count shouldEqual 1

      f.repo.save _ verify fileInfo
    }

    "immediately delete the new uploaded file if a db entry and an identical file exists" in {
      val f = fixture
      val prevFileInfo = FileInfo(storingService, FileHash("dummy"), "dummy", 100, Instant.now())

      f.repo.getFileInfo _ when (*, *) returns Some(prevFileInfo)
      f.repo.save _ when (*) returns (())
      f.objectStorage.put(prevFileInfo.objectPath, fullTestData)

      val fileInfo = await(f.runUpload().map(_.commit))

      fileInfo.objectPath shouldEqual prevFileInfo.objectPath
      f.objectStorage.get(fileInfo.objectPath) shouldEqual Some(fullTestData)
      f.objectStorage.count shouldEqual 1

      (f.repo.save _ verify (*)).never()
    }

    "rollback the upload on user request" in  {
      val f = fixture
      f.repo.getFileInfo _ when (*, *) returns None
      f.repo.save _ when (*) returns (())

      await(f.runUpload().flatMap(_.rollback))

      f.objectStorage.count shouldEqual 0
    }

    "rollback the upload on error" in {
      val f = fixture

      f.repo.getFileInfo _ when (*, *) returns None

      val fileInfoRes =
        Source(testData)
          .map(x => if (x.utf8String == "3") throw TestException else x)
          .runWith(f.uploadSink)
          .map(_.commit)

      intercept[TestException.type]{
        await(fileInfoRes)
      }

      f.objectStorage.count shouldEqual 0
    }

    "downloading empty gzip file creates a valid gzip file" in {
      val f = fixture

      var called = false
      var fileInfoLater: FileInfo = null

      f.repo.getFileInfo _ when (*, *) onCall ((_, _) =>
        if (called) {
          Some(fileInfoLater)
        } else {
          called = true
          None
        }
      )

      val fileInfoRes =
        Source.empty[ByteString]
          .runWith(f.uploadSink)
          .map(_.commit)

      val fileInfo = await(fileInfoRes)
      fileInfoLater = fileInfo

      f.objectStorage.get(fileInfo.objectPath) shouldEqual Some(ByteString.empty)
      await(f.testee.getFileContentGzip(storingService, fileInfo.fileHash).runFold(ByteString.empty)(_ ++ _)) shouldEqual ByteString(31, -117, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -1, -1, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    }

  }

  override def afterAll(): Unit = {
    super.afterAll()
    as.terminate()
    ()
  }

  type Id[T] = T

  implicit val idExecutor: FutureExecutor[Id] = new FutureExecutor[Id] {
    override def transform[A](fa: Id[A]): Future[A] = Future.successful(fa)

    override def pure[A](value: A): Id[A] = value

    override def map[A, B](fa: Id[A])(f: A => B): Id[B] = f(fa)
  }

  def fixture = new {
    implicit val clock: Clock = Clock.systemDefaultZone()
    val repo: FileStorageRepository[Id] = stub[FileStorageRepository[Id]]
    val objectStorage = new SwiftInMemoryMock()
    val testee = new FileStorageServiceImpl(repo, objectStorage)

    val uploadSink = testee.uploadWithoutAutoCommit(storingService)

    def runUpload(): Future[UploadRollbackOrCommit[Id]] = Source(testData).runWith(uploadSink)
  }

}

case object TestException extends Exception("TestException")
