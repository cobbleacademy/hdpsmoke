package com.aslick.strm


// #imports
import java.io.File

import akka.Done
import akka.actor.ActorSystem
import akka.stream.alpakka.slick.scaladsl.Slick
import com.typesafe.config.ConfigFactory
//import akka.actor.typed.ActorSystem
//import akka.actor.typed.scaladsl.Behaviors
import akka.stream.ActorMaterializer
import akka.stream.alpakka.slick.javadsl.SlickSession
import akka.stream.alpakka.slick.scaladsl.Slick
import spray.json.DefaultJsonProtocol.{jsonFormat4, _}
import spray.json.JsonFormat

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}
import akka.stream.alpakka.slick.scaladsl._
import akka.stream.scaladsl._
import slick.jdbc.GetResult

import org.slf4j.LoggerFactory

import java.time

import slick.collection.heterogeneous.{HList, HCons, HNil}
import slick.collection.heterogeneous.syntax._

object ASLKStream {

  final val log = LoggerFactory.getLogger(getClass)

  implicit val system = ActorSystem("system")
  implicit val executionContext = system.dispatcher
  //implicit val materializer = ActorMaterializer()

  def wait(duration: FiniteDuration): Unit = Thread.sleep(duration.toMillis)

  def terminateActorSystem(): Unit = {
    system.terminate()
    Await.result(system.whenTerminated, 1.seconds)
  }

  // #slick-setup
  //import session.profile.api._
  import slick.jdbc.H2Profile.api._

  val confPath = getClass.getResource("/application_slick.conf")
  val config = ConfigFactory.parseFile(new File(confPath.getPath)).resolve()
  //val db = Database.forConfig("slick.db", config)
  val db = Database.forConfig("slick-h2-mem", config)

  //implicit val db = Database.forConfig("slick-h2-mem")
  implicit val session = SlickSession.forConfig("slick-h2")                         // (1)
  system.whenTerminated.map(_ => session.close())


  class Movies(tag: Tag) extends Table[(Int, String, String, Double)](tag, "MOVIE") {   // (2)
    def id = column[Int]("ID")
    def title = column[String]("TITLE")
    def genre = column[String]("GENRE")
    def gross = column[Double]("GROSS")

    override def * = (id, title, genre, gross)
  }
  // #slick-setup
  Await.result(Helper.populateDataForTable()(session, system), 2.seconds)


  // #data-class
  case class Movie(id: Int, title: String, genre: String, gross: Double)

  implicit val format: JsonFormat[Movie] = jsonFormat4(Movie)
  // #data-class
  // We need this to automatically transform result rows
  // into instances of the User class.
  // Please import slick.jdbc.GetResult
  // See also: "http://slick.lightbend.com/doc/3.2.1/sql.html#result-sets"
  implicit val getUserResult = GetResult(r => Movie(r.nextInt(), r.nextString(), r.nextString(), r.nextDouble()))

  //
  // BEGIN: The example sheet domain
  //
  val dropSheetTable =
    sqlu"""drop table if exists SHEET_SINK"""

  val createSheetTable =
    sqlu"""create table SHEET_SINK (ID INT PRIMARY KEY, acol varchar, bcol varchar, ccol varchar, dcol varchar, ecol varchar, fcol varchar, gcol varchar, hcol varchar, icol varchar, jcol varchar, kcol varchar, lcol varchar, mcol varchar, ncol varchar, ocol varchar, pcol varchar, qcol varchar, rcol varchar, scol varchar, tcol varchar, ucol varchar, vcol varchar, wcol varchar, xcol varchar, ycol varchar, zcol varchar)"""

  Await.result(session.db.run(dropSheetTable), 10.seconds)
  Await.result(session.db.run(createSheetTable), 10.seconds)

  case class Sheet(id: Int, acol: String, bcol: String, ccol: String, dcol: String, ecol: String, fcol: String, gcol: String, hcol: String, icol: String, jcol: String, kcol: String, lcol: String, mcol: String, ncol: String, ocol: String, pcol: String, qcol: String, rcol: String, scol: String, tcol: String, ucol: String, vcol: String, wcol: String, xcol: String, ycol: String, zcol: String)

  //(Int, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)
  class Sheets(tag: Tag) extends Table[Sheet](tag, "SHEET_SINK") {   // (2)
    def id = column[Int]("ID")
    def acol = column[String]("ACOL")
    def bcol = column[String]("BCOL")
    def ccol = column[String]("CCOL")
    def dcol = column[String]("DCOL")
    def ecol = column[String]("ECOL")
    def fcol = column[String]("FCOL")
    def gcol = column[String]("GCOL")
    def hcol = column[String]("HCOL")
    def icol = column[String]("ICOL")
    def jcol = column[String]("JCOL")
    def kcol = column[String]("KCOL")
    def lcol = column[String]("LCOL")
    def mcol = column[String]("MCOL")
    def ncol = column[String]("NCOL")
    def ocol = column[String]("OCOL")
    def pcol = column[String]("PCOL")
    def qcol = column[String]("QCOL")
    def rcol = column[String]("RCOL")
    def scol = column[String]("SCOL")
    def tcol = column[String]("TCOL")
    def ucol = column[String]("UCOL")
    def vcol = column[String]("VCOL")
    def wcol = column[String]("WCOL")
    def xcol = column[String]("XCOL")
    def ycol = column[String]("YCOL")
    def zcol = column[String]("ZCOL")

    override def * = (id :: acol :: bcol :: ccol :: dcol :: ecol :: fcol :: gcol :: hcol :: icol :: jcol :: kcol :: lcol :: mcol :: ncol :: ocol :: pcol :: qcol :: rcol :: scol :: tcol :: ucol :: vcol :: wcol :: xcol :: ycol :: zcol :: HNil).mapTo[Sheet]
  }

  val sheetTable = TableQuery[Sheets]

  val sheets = (1 to 42000).map(i => Sheet(i, s"aval$i", s"bval$i", s"cval$i", s"dval$i", s"eval$i", s"fval$i", s"gval$i", s"hval$i", s"ival$i", s"jval$i", s"kval$i", s"lval$i", s"mval$i", s"nval$i", s"oval$i", s"pval$i", s"qval$i", s"rval$i", s"sval$i", s"tval$i", s"uval$i", s"vval$i", s"wval$i", s"xval$i", s"yval$i", s"zval$i"))

  def insertSheet(sheet: Sheet): DBIO[Int] =
    sqlu"INSERT INTO SHEET_SINK VALUES(${sheet.id}, ${sheet.acol}, ${sheet.bcol}, ${sheet.ccol}, ${sheet.dcol}, ${sheet.ecol}, ${sheet.fcol}, ${sheet.gcol}, ${sheet.hcol}, ${sheet.icol}, ${sheet.jcol}, ${sheet.kcol}, ${sheet.lcol}, ${sheet.mcol}, ${sheet.ncol}, ${sheet.ocol}, ${sheet.pcol}, ${sheet.qcol}, ${sheet.rcol}, ${sheet.scol}, ${sheet.tcol}, ${sheet.ucol}, ${sheet.vcol}, ${sheet.wcol}, ${sheet.xcol}, ${sheet.ycol}, ${sheet.zcol})"

//  def bulkInsertSheet(groupSheets: Seq[Sheet]): Future[Sheets] = {
//    //db.run((sheetTable ++= groupSheets)).result(1 sec)
//    db.run(sheetTable ++= groupSheets).map(_.getOrElse(0))
//  }

//  def insertSheet1(sheet: Sheet): DBIO[Int] =
//    sqlu"INSERT INTO SHEET_SINK VALUES(${sheet.id}, ${sheet.acol}, ${sheet.bcol}, ${sheet.ccol}, ${sheet.dcol}, ${sheet.ecol}, ${sheet.fcol}, ${sheet.gcol}, ${sheet.hcol}, ${sheet.icol}, ${sheet.jcol}, ${sheet.kcol}, ${sheet.lcol}, ${sheet.mcol}, ${sheet.ncol}, ${sheet.ocol}, ${sheet.pcol}, ${sheet.qcol}, ${sheet.rcol}, ${sheet.scol}, ${sheet.tcol}, ${sheet.ucol}, ${sheet.vcol}, ${sheet.wcol}, ${sheet.xcol}, ${sheet.ycol}, ${sheet.zcol})"

  //
  // END: The example sheet domain
  //


  //
  // BEGIN: The example sheet domain
  //
  // Definition of the SUPPLIERS table
  class Suppliers(tag: Tag) extends Table[(Int, String, String, String, String, String)](tag, "SUPPLIERS") {
    def id = column[Int]("SUP_ID", O.PrimaryKey) // This is the primary key column
    def name = column[String]("SUP_NAME")
    def street = column[String]("STREET")
    def city = column[String]("CITY")
    def state = column[String]("STATE")
    def zip = column[String]("ZIP")
    // Every table needs a * projection with the same type as the table's type parameter
    def * = (id, name, street, city, state, zip)
  }
  val suppliers = TableQuery[Suppliers]

  class Coffees(tag: Tag) extends Table[(String, Int, Double, Int, Int)](tag, "COFFEES") {
    def name = column[String]("COF_NAME", O.PrimaryKey)
    def supID = column[Int]("SUP_ID")
    def price = column[Double]("PRICE")
    def sales = column[Int]("SALES")
    def total = column[Int]("TOTAL")
    def * = (name, supID, price, sales, total)
    // A reified foreign key relation that can be navigated to create a join
    def supplier = foreignKey("SUP_FK", supID, suppliers)(_.id)
  }
  val coffees = TableQuery[Coffees]

  val setup = DBIO.seq(
    // Create the tables, including primary and foreign keys
    (suppliers.schema ++ coffees.schema).create,

    // Insert some suppliers
    suppliers += (101, "Acme, Inc.",      "99 Market Street", "Groundsville", "CA", "95199"),
    suppliers += ( 49, "Superior Coffee", "1 Party Place",    "Mendocino",    "CA", "95460"),
    suppliers += (150, "The High Ground", "100 Coffee Lane",  "Meadows",      "CA", "93966"),
    // Equivalent SQL code:
    // insert into SUPPLIERS(SUP_ID, SUP_NAME, STREET, CITY, STATE, ZIP) values (?,?,?,?,?,?)

    // Insert some coffees (using JDBC's batch insert feature, if supported by the DB)
    coffees ++= Seq(
      ("Colombian",         101, 7.99, 0, 0),
      ("French_Roast",       49, 8.99, 0, 0),
      ("Espresso",          150, 9.99, 0, 0),
      ("Colombian_Decaf",   101, 8.99, 0, 0),
      ("French_Roast_Decaf", 49, 9.99, 0, 0)
    )
    // Equivalent SQL code:
    // insert into COFFEES(COF_NAME, SUP_ID, PRICE, SALES, TOTAL) values (?,?,?,?,?)
  )

  val setupFuture = db.run(setup)

  //
  // END: The example sheet domain
  //


  def main_movies(args: Array[String]): Unit = {

//    val i: Int = Slick
//      .source(sql"SELECT ID, TITLE, GENRE, GROSS FROM MOVIE".asInstanceOf[WithTrait])

    // Stream the results of a query
    val done: Future[Done] =
      Slick
        .source(sql"SELECT ID, TITLE, GENRE, GROSS FROM MOVIE".as[Movie])
        .log("movie")
        //.runWith(Sink.ignore)
        .runWith( Sink.foreach(println))


    done.failed.foreach(exception => log.error("failure", exception))

    wait(10.seconds)
    terminateActorSystem()

  }


  def protect(data: String, key: String): String = {
    "SGEIGXDG"
  }


  def mapColumns(sheet: Sheet): Sheet = {
    sheet.copy(acol = protect(sheet.acol,"FIRST_NAME"), bcol = sheet.bcol.toUpperCase())
  }


  def main_sheets(args: Array[String]): Unit = {

    // Stream the results of a query
    val done: Future[Done] =
      Source(sheets)
        .map(s => mapColumns(s))
        .grouped(10000)
//        .via(
//          Slick.flow(parallelism = 4, (group: Seq[Sheet]) => group.map(insertSheet(_)).reduceLeft(_.andThen(_)))
//        )
      .mapAsync(2)( (group: Seq[Sheet]) => db.run(sheetTable ++= group).map(_.getOrElse(0)))
//      .map( (group: Seq[Sheet]) => db.run(sheetTable ++= group).map(_.getOrElse(0)))
      .runWith(Sink.foreach(println))

//        .via(
//          // add an optional first argument to specify the parallelism factor (Int)
//          Slick.flow(sheet => insertSheet(sheet))
//        )
//        .log("nr-of-updated-rows")
//        .runWith( Sink.foreach(println))


    done.failed.foreach(exception => log.error("failure", exception))
    done.onComplete(_ => println(time.LocalDateTime.now().toString))

    wait(10.seconds)
    println(time.LocalDateTime.now().toString)

    val q = for (c <- coffees) yield c.name
    val a = q.filter(_.startsWith("Espresso")).result
    val f: Future[Seq[String]] = db.run(a)

    f.onComplete {
      case Success(s) => println(s"Result: $s")
      case Failure(t) => t.printStackTrace()
    }

//    val r = for (c <- sheetTable) yield c.id
//    val b = r.filter(_ > 41980).result
//    val g: Future[Seq[Int]] = db.run(b)
//
//    g.onComplete {
//      case Success(s) => println(s"Result: $s")
//      case Failure(t) => t.printStackTrace()
//    }

    val r = for (c <- sheetTable) yield c
    val b = r.filter(_.id === 41980).result
    val g: Future[Seq[Sheet]] = db.run(b)

    g.onComplete {
      case Success(s) => println(s"Result: $s")
      case Failure(t) => t.printStackTrace()
    }

    terminateActorSystem()

  }


  def main(args: Array[String]): Unit = {
    //main_movies(args)
    main_sheets(args)
  }



//  // #sample
//  val done: Future[Done] =
//    Slick
//      .source(TableQuery[Movies].result)
//      .map {
//        case (id, genre, title, gross) => Movie(id, genre, title, gross)
//      }
//      .map(movie => WriteMessage.createIndexMessage(movie.id.toString, movie))
//      .runWith(ElasticsearchSink.create[Movie](ElasticsearchParams.V7("movie"),
//        ElasticsearchWriteSettings(connection)))

  // #sample
//  done.failed.foreach(exception => log.error("failure", exception))
//  done.onComplete(_ => stopContainers())
//  wait(10.seconds)
//  terminateActorSystem()

}
