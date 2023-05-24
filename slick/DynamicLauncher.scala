package com.aslick.strm


import akka.actor.ActorSystem
import akka.stream.alpakka.slick.javadsl.SlickSession
import com.aslick.strm.Model.JValueOps
import com.aslick.strm.factory.FrameFactory
import com.aslick.strm.factory.FrameFactory.FrameHouse
import com.typesafe.config.ConfigFactory
import org.json4s.native.JsonMethods._
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.reflect.runtime.universe
import scala.tools.reflect.ToolBox




/**
  * A Class to invoke and execute dynamically prepared code externally to run data pipeline
  */
object DynamicLauncher {

  final val logger = LoggerFactory.getLogger(getClass)

  implicit val system = ActorSystem("system")
  implicit val executionContext = system.dispatcher

  def terminationWait(duration: FiniteDuration): Unit = Thread.sleep(duration.toMillis)

  def terminateActorSystem(): Unit = {
    system.terminate()
    Await.result(system.whenTerminated, 1.seconds)
  }


  // #slick-setup
  val conf = ConfigFactory.load

  val srcSession = SlickSession.forConfig(conf.getString("crypto-frame.src-session"))

  val sinkSession = SlickSession.forConfig(conf.getString("crypto-frame.sink-session"))
  //val sinkSession = SlickSession.forConfig(conf.getString("crypto-frame.src-session"))

  system.whenTerminated.map(_ => {srcSession.close(); sinkSession.close();})

  // global initialized variables
  var frameHouse: FrameHouse = _

  //
  val tb = universe.runtimeMirror(getClass.getClassLoader).mkToolBox()



  /**
    * Formatted class fields
    * @return
    */
  def formatClassFields(fields: String, frameName: String): String = {
    //
    var ret = fields

    val fldArr = fields.split(",").map(f => {
      val fdef = f.split(":")
      s"   def ${fdef(0)} = column[${fdef(1).trim}](" + "\"" + s"${fdef(0).toUpperCase.trim}" + "\")"
    })
    ret = fldArr.mkString("\n")
    ret += "\n\n"

    //
    val hlistcols = fields.split(",").map(f => {
      f.split(":")(0)
    })
    val defstr = "  override def * = (" + hlistcols.mkString(" :: ") + " :: HNil).mapTo(scala.reflect.classTag[" + frameName + "])"

    ret += defstr
    ret += "\n"

    //
    ret
  }

  def getFormFieldJValue(ind: Int): String = {
    val msgabc =
      s"""
        |  {
        |    "messages": [
        |      {"profile": { "id":"a${ind}", "score":100, "avatar":"path.jpg" }}
        |      {"profile": { "id":"b${ind}", "score":100, "avatar":"path.jpg" }}
        |    ]
        |  }
        |
      """.stripMargin
    (msgabc.toString)
  }


  def getFormFieldXValue(ind: Int): String = {
    val msgabc =
      s"""
         |  <messages>
         |    <profile>
         |      <id>1</id>
         |      <score>100</score>
         |      <avatar>path.jpg</avatar>
         |    </profile>
         |    <profile>
         |      <id>2</id>
         |      <score>200</score>
         |      <avatar>path.jpg</avatar>
         |    </profile>
         |  </messages>
      """.stripMargin
    (msgabc.toString)
  }


  /**
    * Main Building Factory entry point
    * @param args
    */
  def main_pipeline(args: Array[String]): Unit = {
    val toolbox = tb

    println("FrameController dynamic class is getting parsed...")

    val srcCaseString = frameHouse.getSourceFields()
    val srcTableName = frameHouse.getSourceReader()
    val srcFmtClsFields = formatClassFields(srcCaseString, "SourceFrame")
    //println(srcFmtClsFields)
    val flowGroupSize = frameHouse.getBatchSize()
    val flowAsyncCount = frameHouse.getParallelismCount()
    val sinkCaseString = frameHouse.getSinkFields()
    val sinkTableName = frameHouse.getSinkWriter()
    val sinkFmtClsFields = formatClassFields(sinkCaseString, "SinkFrame")
    val sinkDelete = frameHouse.isSinkDelete()
    val sinkDeleteString = frameHouse.getSinkDelete()
    val sinkCreate = frameHouse.isSinkCreate()
    val sinkCreateStringRaw = frameHouse.getSinkCreate()
    var sinkCreateString = sinkCreateStringRaw
    if(sinkCreateStringRaw.isEmpty)
      sinkCreateString = s"create table ${sinkTableName}(${sinkCaseString})"
    val sinkMapSourceString = frameHouse.mapSourceSinkFields()


    //
    // Prepare Dynamic Frame Controller class
    //
    val frameControllerClass = toolbox.compile(
      tb.parse(
        s"""
           |
           |  import com.aslick.strm._
           |  import com.aslick.strm.DynamicLauncher._
           |  import com.aslick.strm.Model.JValueOps
           |  import com.aslick.strm.BaseCryptoCodec.{protect, unprotect}
           |  import com.aslick.strm.Model.{jsonCompactRender, jsonProtect, jsonUnprotect, xmlProtect, xmlUnprotect}
           |
           |  import java.io.File
           |  import java.time.LocalDateTime
           |
           |  import akka.Done
           |  import akka.actor.ActorSystem
           |  import akka.stream.alpakka.slick.javadsl.SlickSession
           |  import akka.stream.alpakka.slick.scaladsl._
           |  import akka.stream.scaladsl._
           |
           |  import scala.concurrent.duration._
           |  import scala.concurrent.{Await, ExecutionContext, Future}
           |  import scala.reflect.runtime.universe
           |  import scala.tools.reflect.ToolBox
           |  import scala.util.{Failure, Success}
           |
           |  import slick.collection.heterogeneous.{HCons, HList, HNil}
           |  import slick.jdbc.GetResult
           |
           |  import slick.jdbc.H2Profile.api._
           |
           |
           |
           |  object FrameController {
           |
           |    case class SourceFrame(${srcCaseString})
           |
           |    class SourceFrames(tag: Tag) extends Table[SourceFrame](tag, "${srcTableName}") {
           |      ${srcFmtClsFields}
           |    }
           |
           |    //val dropSrcFrame = sqlu" drop table if exists FRAME_SRC"
           |    //val createSrcFrame = sqlu" create table FRAME_SRC(${sinkCaseString})"
           |
           |    //Await.result(srcSession.db.run(dropSrcFrame), 10.seconds)
           |    //Await.result(srcSession.db.run(createSrcFrame), 10.seconds)
           |
           |    val sourceTable = TableQuery[SourceFrames]
           |
           |    val sourceFrames = (1 to 42000).map(i => SourceFrame(i, s"aval$$i", s"bval$$i", "" + getFormFieldJValue(i) + "", "" + getFormFieldXValue(i) + "", s"eval$$i", s"fval$$i", s"gval$$i", s"hval$$i", s"ival$$i", s"jval$$i", s"kval$$i", s"lval$$i", s"mval$$i", s"nval$$i", s"oval$$i", s"pval$$i", s"qval$$i", s"rval$$i", s"sval$$i", s"tval$$i", s"uval$$i", s"vval$$i", s"wval$$i", s"xval$$i", s"yval$$i", s"zval$$i"))
           |
           |    case class SinkFrame(${sinkCaseString})
           |
           |    class SinkFrames(tag: Tag) extends Table[SinkFrame](tag, "${sinkTableName}") {
           |      ${sinkFmtClsFields}
           |    }
           |
           |    val dropSinkFrame = sqlu" drop table if exists ${sinkTableName}"
           |    val createSinkFrame = sqlu" ${sinkCreateString}"
           |
           |
           |    if($sinkDelete) Await.result(sinkSession.db.run(dropSinkFrame), 10.seconds)
           |    if($sinkCreate) Await.result(sinkSession.db.run(createSinkFrame), 10.seconds)
           |
           |    val sinkTable = TableQuery[SinkFrames]
           |
           |    def mapSourceSinkColumns(s: SourceFrame): SinkFrame = {
           |      ${sinkMapSourceString}
           |    }
           |
           |    def main(args: Array[String]): Unit = {
           |      //
           |      println("FrameController::main() is invoked")
           |      println(LocalDateTime.now().toString)
           |
           |      implicit val session = srcSession
           |
           |      // Stream the results of a query
           |      val done: Future[Done] =
           |          //Source(sourceFrames)
           |          Slick.source(sourceTable.result)
           |            .map(s => mapSourceSinkColumns(s))
           |            .grouped($flowGroupSize)
           |            .mapAsync($flowAsyncCount)( (group: Seq[SinkFrame]) => sinkSession.db.run(sinkTable ++= group).map(_.getOrElse(0)))
           |            .runWith(Sink.foreach(println))
           |
           |      //
           |      done.failed.foreach(exception => logger.error("failure", exception))
           |      done.onComplete(_ => {
           |        println(LocalDateTime.now().toString)
           |        val r = for (c <- sinkTable) yield c
           |        val b = r.filter(_.id === 41980).result
           |        val g: Future[Seq[SinkFrame]] = sinkSession.db.run(b)
           |
           |        g.onComplete {
           |          case Success(s) => println(s"Result: $$s")
           |          case Failure(t) => t.printStackTrace()
           |        }
           |      })
           |
           |      terminationWait(10.seconds)
           |      println(LocalDateTime.now().toString)
           |    }
           |
           |
           |    def main_print(args: Array[String]): Unit = {
           |      println("FrameController::main_print is executed.")
           |    }
           |
           | }
           | //scala.reflect.classTag[FrameController].runtimeClass
           |
           | FrameController.main(Array())
           | //FrameController.main_print(Array())
           |
           |
         """.stripMargin))

    //
    frameControllerClass()

    println ("FrameController dynamic class is loaded.")

  }


  /**
    * Main Building Factory entry point
    * @param args
    */
  def main_framehouse(args: Array[String]): Unit = {
    FrameFactory.initialize("props/jdbc_pipeline.code", tb)
    frameHouse = FrameFactory.getFrameHouse()
    println(frameHouse.getSourceFields())
  }

  /**
    * Main Building Factory entry point
    * @param args
    */
  def main_json(args: Array[String]): Unit = {

    val JSONString = """
      {
         "name":"luca",
         "id": "1q2w3e4r5t",
         "age": 26,
         "url":"http://www.nosqlnocry.wordpress.com",
         "url":"https://nosqlnocry.wordpress.com",
         "loginTimeStamps": [1434904257,1400689856,1396629056],
         "messages": [
          {"id":1,"content":"Please like this post!"},
          {"id":2,"content":"Forza Roma!"}
         ],
         "profile": { "id":"my-nickname", "score":123, "avatar":"path.jpg" }
      }
      """

    val customjson = """ {"id":1} """
    //"\\\\\""
    val myjson = customjson//.replaceAll("\"","\\\\\\\\\"")
      //JSONString.replaceAll("\"","\\\\\\\\\"")
    println(myjson)

    val JSON = parse(JSONString)
    val joopsstr = "profile.avatar:FIRST_NAME"
    //val jops = JSON.jsonProtect(joopssplits.toList, "FIRST_NAME")//JString("new value"))
    val jops = JSON.jsonReplaceProtect(joopsstr)//JString("new value"))
    println(compact(render(jops)))
    println(getFormFieldJValue(1001))
    println("+++++++++++++++++++++++++++++++++")

    //val upAvatar = JSON transformField {
    //  case JField("avatar", JString(s)) => ("avatar", JString(s.toUpperCase))
    //}
    //println(compact(render(upAvatar)))

    //println(JSON)
    //println(compact(render(JSON)))
    //println(JSON.\\("id"))
    //println(JSON\"id")

    //val newIdJson =
    //  JSON transformField {
    //    case JField("id", JString(s)) => ("id", JString(s.toUpperCase))
    //  }

    //println(newIdJson)
    //println(JSON.replace("messages[0]" :: "id" :: Nil, JInt(100)))
    val a = 1
    val b = 2

    val evalId = tb.eval(
      tb.parse(
        s"""
           |  import org.json4s._
           |  import org.json4s.native.JsonMethods._
           |  import com.aslick.strm.Model.JValueOps
           |  import com.aslick.strm.DynamicLauncher.getFormFieldValue
           |
           |
           |  object JsonHello {
           |
           |    def addition(p: Int, q:Int): Int = {
           |      p + q
           |    }
           |
           |    def getJsonField(jsonstr: String): JValue = {
           |      val injson1 = parse(jsonstr)
           |      val injson = injson1.jsonReplaceProtect("${joopsstr}")
           |      println(compact(render(injson)))
           |      injson
           |    }
           |
           |    def main(args: Array[String]): Unit = {
           |      println(addition(${a}, ${b}))
           |      val mjson1 = parse("" + getFormFieldValue(1001) + "")
           |      val mjson = mjson1.jsonReplaceProtect("messages[1].profile.avatar:FIRST_NAME")
           |      println(compact(render(mjson)))
           |      println(getJsonField("{\\"id\\":1}"))
           |      getJsonField(\"\"\"$JSONString\"\"\")
           |    }
           |
           |  }
           |
           |  JsonHello.main(Array())
       """.stripMargin))

    //evalId

  }


  /**
    * Main Building Factory entry point
    * @param args
    */
  def initialize(args: Array[String]): Unit = {
    main_framehouse(args)
  }

  /**
    * Entry point for the program
    * @param args
    */
  def main(args: Array[String]): Unit = {
    //
    initialize(args)

    //
    try {
      main_pipeline(args)
      //main_json(args)
      //main_conv_agg(args)
    } catch {
      case e: Exception => {println(e.printStackTrace())}
      case t: Throwable => println("Failed..." + t.printStackTrace())
    }

    //
    terminateActorSystem()
  }

}
