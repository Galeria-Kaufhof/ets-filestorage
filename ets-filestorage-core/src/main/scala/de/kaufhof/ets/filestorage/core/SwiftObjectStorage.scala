package de.kaufhof.ets.filestorage.core

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.{Http, HttpExt}
import akka.http.scaladsl.coding.Gzip
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.ByteString
import de.kaufhof.ess.eval.common.streamutils.StreamUtils

import scala.concurrent.Future
import scala.concurrent.stm.Ref
import scala.collection.immutable

case class ObjectNotExistingException(objectName: String) extends Exception(s"Object does not exist: $objectName")

trait SwiftObjectStorage {
  def upload(objectPath: String, src: Source[ByteString, NotUsed]): Future[Unit]
  def download(objectPath: String): Source[ByteString, NotUsed]
  def downloadGzip(objectPath: String): Source[ByteString, NotUsed]
  def delete(objectPath: String): Future[Unit]
  def exists(objectPath: String): Future[Boolean]
}

class SwiftObjectStorageImpl(config: SwiftConfig)(implicit as: ActorSystem, mat: Materializer) extends SwiftObjectStorage with ObjectstoreLogging {

  import as.dispatcher
  implicit protected val http: HttpExt = Http()

  protected val auth = new SwiftAuth(config)
  protected val authToken = Ref(Option.empty[SwiftToken])

  // Why always gzip mediatype? For some unknown reasons the content-enconding header is not set on swift PUT operation (at GKH?).
  // We upload all files gzipped, thus the media-type is always gzip
  protected val uploadMediaType: MediaType.Binary = MediaTypes.`application/x-gzip`

  override def upload(objectPath: String, src: Source[ByteString, NotUsed]): Future[Unit] = {

    for {
      //catch uri exceptions inside async context
      uri      <- Future(createUri(objectPath))
      request   = SwiftRequest(HttpMethods.PUT, uri, HttpEntity(ContentType(uploadMediaType), src.via(StreamUtils.gzipEncodeFlow())))
      response <- requestWithAuth(request)
      responseBytes <- response.entity.dataBytes.runWith(Sink.fold(ByteString.empty)(_ ++ _))
    } yield {
      if (response.status == StatusCodes.Created || response.status == StatusCodes.OK || response.status == StatusCodes.Accepted) {
        ()
      } else {
        log.error(s"SWIFT: Upload failed with status code ${response.status.value} and response: ${responseBytes.utf8String}")
        throw new Exception(s"Upload failed with status code ${response.status.value}")
      }
    }
  }

  override def download(objectPath: String): Source[ByteString, NotUsed] =
    downloadGzip(objectPath).via(Gzip.decoderFlow)

  override def downloadGzip(objectPath: String): Source[ByteString, NotUsed] = {

    val futureSource = for {
      //catch uri exceptions inside async context
      uri      <- Future(createUri(objectPath))
      request   = SwiftRequest(HttpMethods.GET, uri)
      response <- requestWithAuth(request)
    } yield {
      if (response.status == StatusCodes.OK) {
        response.entity.dataBytes
      } else if (response.status == StatusCodes.NotFound) {
        Source.failed(ObjectNotExistingException(objectPath))
      } else {
        Source.fromFuture(
          response.entity.dataBytes.runWith(Sink.fold(ByteString.empty)(_ ++ _)).map(responseBytes =>
            throw new Exception(s"SWIFT: Download failed with status code ${response.status}: ${responseBytes.utf8String}")
          )
        )
      }
    }

    Source.fromFuture(futureSource).flatMapConcat(identity)
  }

  override def delete(objectPath: String): Future[Unit] = {
    for {
      uri      <- Future(createUri(objectPath))
      request   = SwiftRequest(HttpMethods.DELETE, uri)
      response <- requestWithAuth(request)
    } yield {
      if (response.status == StatusCodes.OK || response.status == StatusCodes.NoContent) {
        ()
      } else {
        throw new Exception(s"Unexpected status code ${response.status.value} when deleting")
      }
    }
  }

  override def exists(objectPath: String): Future[Boolean] = {
    for {
      uri      <- Future(createUri(objectPath))
      request   = SwiftRequest(HttpMethods.HEAD, uri)
      response <- requestWithAuth(request)
    } yield {
      if (response.status == StatusCodes.OK || response.status == StatusCodes.NoContent) {
        true
      } else if (response.status == StatusCodes.NotFound) {
        false
      } else {
        throw new Exception(s"Unexpected status code ${response.status.value} when checking if object exists")
      }
    }
  }

  protected def createUri(objectPath: String): Uri = {
    val cleanPath = objectPath.stripPrefix("/")
    require(cleanPath.nonEmpty, "No object path given")
    Uri(cleanPath)
  }

  protected def getToken: Future[SwiftToken] =
    authToken.single().map(Future.successful).getOrElse(auth.authorize)

  protected def refreshToken: Future[Unit] =
    auth.authorize.map(token => authToken.single() = Some(token))

  protected def requestWithAuth(req: SwiftRequest, retriesLeft: Int = 1): Future[HttpResponse] = {
    def createReqWithAuth = getToken.map(token =>
      req
        .copy(uri = token.publicContainerUrl.withPath(token.publicContainerUrl.path + "/" ++ req.uri.path))
        .copy(headers = req.headers ++ Seq(RawHeader("X-Auth-Token", token.token)))
        .toHttpRequest
    )

    createReqWithAuth.flatMap(http.singleRequest(_)).flatMap{resp =>
      if (resp.status == StatusCodes.Unauthorized) {
        if (retriesLeft > 0) {
          resp.entity.discardBytes()
          requestWithAuth(req, retriesLeft - 1)
        } else {
          resp.entity.dataBytes.runWith(Sink.fold(ByteString.empty)(_ ++ _)).foreach(errorResponse =>
            log.error(s"Swift authentication failed: ${errorResponse.utf8String}" )
          )
          Future.failed(new Exception("Swift authentication failed"))
        }
      } else {
        Future.successful(resp)
      }
    }
  }
}

case class SwiftConfig(authUrl: Uri, user: String, password: String, containerName: String)

object SwiftConfig {
  def apply(url: String): SwiftConfig = {
    val uri = Uri(url)
    val List(user, pass) = uri.authority.userinfo.split(':').toList
    val containerName = uri.query().find(_._1 == "bucket").map(_._2).get
    val authUrl = uri.withUserInfo("").withQuery(Query.Empty)
    SwiftConfig(authUrl, user, pass, containerName)
  }
}

private[core] case class SwiftToken(token: String, publicContainerUrl: Uri)
private[core] case class SwiftRequest(method: HttpMethod, uri: Uri, entity: RequestEntity = HttpEntity.Empty, headers: immutable.Seq[HttpHeader] = Nil) {
  def toHttpRequest: HttpRequest = HttpRequest(method, uri, headers, entity)
}

