package de.kaufhof.ets.filestorage.core

import akka.actor.ActorSystem
import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model._
import akka.stream.Materializer
import play.api.libs.json._
import play.api.libs.functional.syntax._

import scala.concurrent.Future
import scala.concurrent.duration._

private[core] class SwiftAuth(config: SwiftConfig)(implicit as: ActorSystem, mat: Materializer, http: HttpExt) extends ObjectstoreLogging {

  import as.dispatcher

  protected val authTimeout: FiniteDuration = 10.seconds

  protected val request: HttpRequest = {
    val body =
      s"""
         |{"auth": {
         |    "identity": {
         |      "methods": ["password"],
         |      "password": {
         |        "user": {
         |          "name": "${config.user}",
         |          "domain": { "id": "default" },
         |          "password": "${config.password}"
         |        }
         |      }
         |    },
         |    "scope": {
         |      "project": {
         |        "name": "${config.containerName}",
         |        "domain": { "id": "default" }
         |      }
         |    }
         |  }
         |}
      """.stripMargin

    HttpRequest(HttpMethods.POST, config.authUrl, entity = HttpEntity(ContentTypes.`application/json`, body))
  }

  def authorize: Future[SwiftToken] = {

    for {
      response <- http.singleRequest(request)
      body     <- response.entity.toStrict(authTimeout)
    } yield {
      val bodyString = body.getData().utf8String
      if (response.status == StatusCodes.OK || response.status == StatusCodes.Created) {
        Json.parse(bodyString).validate[Auth] match {
          case JsSuccess(auth, _) =>
            val tokenOpt = for {
              catalogue <- auth.catalogEntries.find(_.`type` == "object-store")
              endpoint  <- catalogue.endpoints.find(_.interface == "public")
              token    <- response.headers.find(_.lowercaseName == "x-subject-token").map(_.value)
            } yield SwiftToken(token, Uri(endpoint.url + "/" + config.containerName))

            tokenOpt match {
              case Some(token) => token
              case None =>
                log.error("SWIFT authentication failed: endpoint or token not found")
                throw new Exception("SWIFT authentication failed: endpoint or token not found")
            }
          case JsError(errors) =>
            log.error(s"SWIFT authentication failed when parsing json: $errors")
            throw new Exception("SWIFT authentication: parsing auth json failed")
        }
      } else {
        log.error(s"SWIFT authentication failed due to unexcepted status code ${response.status} and response: $bodyString")
        throw new Exception(s"SWIFT authentication: unexpected status code ${response.status}")
      }
    }

  }
}

private[core] case class Endpoint(url: String, interface: String, region: String, regionId: String, id: String)
private[core] case class CatalogEntry(id: String, `type`: String, name: String, endpoints: Seq[Endpoint])
private[core] case class Auth(id: String, catalogEntries: Seq[CatalogEntry])

private[core] object Endpoint {
  implicit val endpointReads: Reads[Endpoint] = (
    (JsPath \ "url").read[String] and
      ((JsPath \ "interface").read[String] orElse Reads.pure("")) and
      ((JsPath \ "region").read[String] orElse Reads.pure("")) and
      ((JsPath \ "Endpoint").read[String] orElse Reads.pure("")) and
      ((JsPath \ "id").read[String] orElse Reads.pure(""))
    )(Endpoint.apply _)
}

private[core] object CatalogEntry {
  implicit val catalogEntryReads: Reads[CatalogEntry] = (
    (JsPath \ "id").read[String] and
      (JsPath \ "type").read[String] and
      (JsPath \ "name").read[String] and
      (JsPath \ "endpoints").read[Seq[Endpoint]]
    )(CatalogEntry.apply _)
}

private[core] object Auth {
  implicit val authReads: Reads[Auth] = (
    (JsPath \ "token" \ "project" \ "id").read[String] and
      (JsPath \ "token" \ "catalog").read[Seq[CatalogEntry]]
    )(Auth.apply _)
}
