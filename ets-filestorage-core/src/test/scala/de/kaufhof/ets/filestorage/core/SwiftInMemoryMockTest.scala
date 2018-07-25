package de.kaufhof.ets.filestorage.core

import akka.actor.ActorSystem
import akka.http.scaladsl.coding.Gzip
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.util.ByteString
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Try}

class SwiftInMemoryMockTest extends WordSpec with Matchers with MockFactory with BeforeAndAfterAll {

  implicit val as = ActorSystem("FileStorageServiceImplTest")
  import as.dispatcher
  implicit val mat = ActorMaterializer()

  def await[T](a: Future[T]): T = Await.result(a, 5.seconds)
  def awaitCatch[T](a: Future[T]): Try[T] = Try(Await.result(a, 5.seconds))

  val testPath = "testpath"
  val content = ByteString("MyTestContent")

  "SwiftInMemoryMock" should {

    "transparently gzip content when using upload" in {
      val testee = new SwiftInMemoryMock()

      await(testee.upload(testPath, Source.single(content)))

      testee.memoryStore.single().get(testPath).get shouldNot be(content)
      await(Gzip.decode(testee.memoryStore.single().get(testPath).get)) shouldEqual content
    }

    "transparently gzip content when using put" in {
      val testee = new SwiftInMemoryMock()

      testee.put(testPath, content)

      testee.memoryStore.single().get(testPath).get shouldNot be(content)
      await(Gzip.decode(testee.memoryStore.single().get(testPath).get)) shouldEqual content
    }

    "complete download/upload/delete loop" in {
      val testee = new SwiftInMemoryMock()

      await(testee.upload(testPath, Source.single(content)))

      await(testee.get(testPath)) shouldEqual Some(content)
      await(testee.download(testPath).runFold(ByteString.empty)(_ ++ _)) shouldEqual content

      await(testee.delete(testPath))

      await(testee.get(testPath)) shouldEqual None
      awaitCatch(testee.download(testPath).runFold(ByteString.empty)(_ ++ _)) shouldEqual Failure(ObjectNotExistingException(testPath))

      testee.put(testPath, content)

      await(testee.get(testPath)) shouldEqual Some(content)
      await(testee.download(testPath).runFold(ByteString.empty)(_ ++ _)) shouldEqual content
    }

  }

  override def afterAll(): Unit = {
    super.afterAll()
    as.terminate()
    ()
  }


}
