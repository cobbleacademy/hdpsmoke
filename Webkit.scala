package com.akkarest.service

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.Done
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import spray.json.DefaultJsonProtocol._
import spray.json._
import java.sql.{DriverManager, PreparedStatement, ResultSet, Statement}

import com.akkarest.service.Models.PatternRequest

import scala.collection.mutable.ListBuffer
import scala.io.StdIn
import scala.concurrent.Future

object Webkit {

  // needed to run the route
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  // needed for the future map/flatmap in the end and future in fetchItem and saveOrder
  implicit val executionContext = system.dispatcher

  val Driver  = "org.apache.phoenix.jdbc.PhoenixDriver"

  val USE = """ use "dream" """
  val QUERY = """ select * from "languages" """

  var orders: List[Item] = List(Item("Ipad",100))
  var languages = new ListBuffer[Language]()

  // domain model
  final case class Language(id: String, name: String)
  final case class LanguageList(languages: List[Language])
  final case class Item(name: String, id: Long)
  final case class Order(items: List[Item])

  // formats for unmarshalling and marshalling
  implicit val languageFormat = jsonFormat2(Language)
  implicit val languageListFormat = jsonFormat1(LanguageList)
  implicit val itemFormat = jsonFormat2(Item)
  implicit val orderFormat = jsonFormat1(Order)
  implicit val patternRequestFormat = jsonFormat2(PatternRequest)

  // (fake) async database query api
  def fetchItem(itemId: Long): Future[Option[Item]] = Future {
    orders.find(o => o.id == itemId)
  }
  def saveOrder(order: Order): Future[Done] = {
    orders = order match {
      case Order(items) => items ::: orders
      case _            => orders
    }
    Future { Done }
  }
  def fetchLanguageList(): Future[LanguageList] = Future {
    LanguageList(languages.toList)
  }

  def main(args: Array[String]) {

    //
    PropsUtil.initialize()

    // parse the string
    val source = """{ "pk": "1", "name": "" }"""
    val jsonAst = source.parseJson // or JsonParser(source)
    val patReq: PatternRequest = jsonAst.convertTo[PatternRequest]

    // find the member pattern seq
    PropsUtil.findMemPattern(patReq)


    //
    Class.forName(Driver)
    //jdbc:phoenix:localhost:2181:/hbase
    val conn = DriverManager.getConnection("jdbc:phoenix:localhost:2181:/hbase")

    val usestmt: Statement = conn.createStatement()
    usestmt.execute(USE)

    val pstmt: PreparedStatement = conn.prepareStatement(QUERY)
    val rs: ResultSet = pstmt.executeQuery

    while (rs.next()) {
      val pk = rs.getString("PK")
      val nm = rs.getString("NAME")
      languages += Language(pk, nm)
      println(s"0 = ${pk}, " + s"1 = ${nm}" )
    }


    val route: Route =
      get {
        path("languages") {
          // there might be no item for a given id
          val maybeItem: Future[LanguageList] = fetchLanguageList

          onSuccess(maybeItem) {
            case lst => complete(lst)
            case _    => complete(StatusCodes.NotFound)
          }
        }
      } ~
      get {
        pathPrefix("item" / LongNumber) { id =>
          // there might be no item for a given id
          val maybeItem: Future[Option[Item]] = fetchItem(id)

          onSuccess(maybeItem) {
            case Some(item) => complete(item)
            case None       => complete(StatusCodes.NotFound)
          }
        }
      } ~
      post {
        path("create-order") {
          entity(as[Order]) { order =>
            val saved: Future[Done] = saveOrder(order)
            onComplete(saved) { done =>
              complete("order created")
            }
          }
        }
      }

    val bindingFuture = Http().bindAndHandle(route, "localhost", 8080)
    println(s"Server item-orders online at http://localhost:8080/\nPress RETURN to stop...")
    StdIn.readLine() // let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ ⇒ system.terminate()) // and shutdown when done

  }
}