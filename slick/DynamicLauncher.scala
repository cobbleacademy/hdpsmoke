package com.aslick.strm


import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file._
import java.nio.charset.{Charset, StandardCharsets}

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.alpakka.slick.javadsl.SlickSession
import akka.stream.scaladsl.Sink
import akka.util.ByteString

import com.aslick.strm.BaseCryptoCodec.protect
import com.aslick.strm.Model.{BaseExecutor, BaseFrame, JValueOps}
import com.aslick.strm.factory.FrameFactory
import com.aslick.strm.factory.FrameFactory.{FrameHouse, FrameHouseExecutor}
import com.aslick.strm.DynamicLauncher.frameHouse

import com.typesafe.config.ConfigFactory

import org.json4s.native.JsonMethods._
import org.slf4j.LoggerFactory

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.io.Source
import scala.reflect.runtime.universe
import scala.tools.reflect.ToolBox
import scala.util.Try

import akka.stream.scaladsl._
import akka.stream.IOResult
import akka.util.ByteString
import akka.stream.alpakka.csv.scaladsl.{CsvFormatting, CsvParsing, CsvQuotingStyle, CsvToMap}





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
    * Returns a map from  given string with , and :
    * @param mapStr
    * @return
    */
  def stringToMap(mapStr: String): Map[String, String] = {
    mapStr.split(",").map(f => f.split(":")).map(t => t(0) -> t(1)).toMap
  }

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
    //FrameFactory.initialize("props/jdbc_pipeline.code", tb)
    FrameFactory.initialize("props/file_db_pipeline.settings", tb)
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

  abstract class EvalHello() {
    def process(a: Int, b: Int): Int
  }


  /**
    * Main Building Factory entry point
    * @param args
    */
  def main_eval_hello(args: Array[String]): Unit = {

    var baseFrame: BaseFrame = null

    // Load Settings
//    val fileSettings = Source.fromFile("props/file_db_pipeline.settings").getLines.mkString("\n")
//    //println(fileSettings)
//    val settingsTree = tb.parse(fileSettings)
//    val compiledCode = tb.compile(settingsTree)
//    baseFrame = compiledCode().asInstanceOf[BaseFrame]

//    println("Employee dynamic class is getting parsed...")
//    val sourceClass = tb.eval(
//      tb.parse(
//        s"""
//           |
//           | case class Employee(_id: Int, _name: String) extends com.aslick.strm.BoxTrait with com.aslick.strm.WithTrait {
//           |    def id = _id
//           |    def name = _name
//           | }
//           | scala.reflect.classTag[Employee].runtimeClass
//       """.stripMargin))
//      .asInstanceOf[Class[_]]
//    println("Employee dynamic class is loaded.")
//
//    println("Dynamic Employee class name: " + sourceClass.getName)

//    val fileContents = Source.fromFile("props/FileDB.template").getLines.mkString("\n")
//    println(fileContents)
//
//    val codeTree = tb.parse(fileContents)
//    //val clsDef: scala.reflect.api.Trees.ClassDef = codeTree.asInstanceOf[ClassDef]
//    val clz = tb.eval(codeTree).asInstanceOf[Class[_]]
//    val ctor = clz.getDeclaredConstructors()(0)
//    val instance: BaseExecutor = ctor.newInstance(baseFrame).asInstanceOf[BaseExecutor]
//    instance.execute()

//    val fileContents = Source.fromFile("props/eval_hello_pipeline.code").getLines.mkString("\n")
//    println(fileContents)
//
    import akka.stream.alpakka.csv.scaladsl.CsvParsing
    import java.time.LocalDateTime

    val kvMap: Map[String, String] = Map("City" -> "FIRST_NAME","State" -> "FIRST_NAME")
    println(kvMap.contains("City"))

//    val done1: Future[Done] =
//    akka.stream.scaladsl.Source.single(ByteString("eins,zwei,drei\n"))
//      .via(CsvParsing.lineScanner())
//      .map(_.map(_.utf8String))
//      .runWith(Sink.foreach(println)) //Sink.head)

//    val done1: Future[Done] =
//      FileIO.fromPath(Paths.get("input/cities.csv"))
//        //.via(CsvFormatting.format())
//        .via(CsvParsing.lineScanner())
//        .via(CsvToMap.toMapAsStrings(StandardCharsets.UTF_8))
//        //.via(CsvParsing.lineScanner())
//        .map(_.map {case (key, value) => (key.trim.replaceAll("\"",""), value)})
//        .map(_.map(t => {
//          var newmap = t
//          //println(t._1 + " +++ " + kvMap.contains(t._1))
//          if (kvMap.contains(t._1)) newmap = (t._1, protect("x", "XYZ"))
//          newmap
//        }))
//        .runWith(Sink.foreach(println)) //Sink.head)

//    val done1: Future[Done] =
//      akka.stream.scaladsl.Source
//        .single(List("eins", "zwei", "drei"))
//        .via(CsvFormatting.format())
//        .map(t => t.utf8String)
//        .runWith(Sink.foreach(println)) //Sink.foreach(println)) //Sink.head)

    var headers = ""
//    val done2: Future[IOResult] =
//      akka.stream.scaladsl.Source(List("name", "apple", "orange", "banana"))
//        .zipWithIndex
//        .filter(t => (t._2 == 0))
//        .map(t => {
//          headers = t._1
//          t._1
//        })
//        .map(t => ByteString(t))
//        .runWith(FileIO.toPath(Paths.get("output/fruits.csv"), Set(StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE)))

    val done2: Future[IOResult] =
      FileIO.fromPath(Paths.get("input/cities.csv"))
        .via(CsvParsing.lineScanner())
        .map(_.map(_.utf8String))
        .zipWithIndex
        .filter(_._2 == 0)
        .map(t => ByteString(t._1.mkString))
        .runWith(FileIO.toPath(Paths.get("output/fruits.csv"), Set(StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE)))

    //
    done2.failed.foreach(exception => logger.error("failure", exception))
    done2.onComplete(_ => {
      println(LocalDateTime.now().toString)

      val done1: Future[IOResult] =
        akka.stream.scaladsl.Source(List("name", "apple", "orange", "banana"))
          .zipWithIndex
          .filter(_._2 != 0)
          .map(t => t._1)
          .map(t => ByteString("\n" + t))
          .runWith(FileIO.toPath(Paths.get("output/fruits.csv"), Set(StandardOpenOption.APPEND, StandardOpenOption.WRITE)))


      //
      done1.failed.foreach(exception => logger.error("failure", exception))
      done1.onComplete(_ => {
        println(LocalDateTime.now().toString)
      })

    })


    terminationWait(10.seconds)
    println(LocalDateTime.now().toString)
    println("++++++++++++++++++")

    val a = 1
    val b = 2

    //val evalId = tb.compile(
      //tb.parse("" + fileContents + ""))


//    val evalId = tb.eval(
//      tb.parse(
//        s"""
//           |  import org.json4s._
//           |  import org.json4s.native.JsonMethods._
//           |  import com.aslick.strm.Model.JValueOps
//           |
//           |
//           |  object EvalHello {
//           |
//           |    def addition(p: Int, q:Int): Int = {
//           |      p + q
//           |    }
//           |
//           |    def main(args: Array[String]): Unit = {
//           |      println(addition(${a}, ${b}))
//           |    }
//           |
//           |  }
//           |
//           |  EvalHello.main(Array())
//       """.stripMargin))

    //val evalHello = evalId().asInstanceOf[EvalHello]

    //println(evalHello.process(a, b))

  }


  /**
    * Main Building Factory entry point
    * @param args
    */
  def getFileTemplateContents(fTmpl: String, fsTmpl: String): String = {

    val fileTemplate = Source.fromFile(fTmpl).getLines.mkString("\n").split("\n")
    println("FileFile.template")
    println("+++++++++++++++++++++++++")
    //println(fileTemplate.mkString("\n"))
    println("+++++++++++++++++++++++++")

    //
    val fileSubTemplate = Source.fromFile(fsTmpl).getLines.mkString("\n").split("\n")
    println("FileFile.subtemplate")
    println("+++++++++++++++++++++++++")
    //println(fileSubTemplate.mkString("\n"))
    println("+++++++++++++++++++++++++")


    //
    val finalTemplate = fileTemplate.map(line => {
      var finalLine = line
      fileSubTemplate.foreach(sub => {
        val subSplts = sub.split(":")
        if (line.indexOf(subSplts(0)) > -1) {
          //println()
          val mtd = frameHouse.getClass.getMethod(subSplts(1))
          val mtdRes = Try {mtd.invoke(frameHouse)}.recover { case _ => ()}.get.toString
          finalLine = line.replaceAllLiterally(subSplts(0), mtdRes)
        }
      })
      finalLine
    })

    println("Final FileFile template")
    println("+++++++++++++++++++++++++")
    println(finalTemplate.mkString("\n"))
    println("+++++++++++++++++++++++++")


    //
    finalTemplate.mkString("\n")
  }

  /**
    * Main Building Factory entry point
    * @param args
    */
  def main_file_util(args: Array[String]): Unit = {
    getFileTemplateContents("props/FileFile.template", "props/FileFile.subtemplate")
  }


    /**
    * Main Building Factory entry point
    * @param args
    */
  def main_template_pipeline(args: Array[String]): Unit = {
    val toolbox = tb

    println("FrameController dynamic class is getting parsed...")

    val srcsinkType = s"${frameHouse.getSourceType}${frameHouse.getSinkType}"

    var frameControllerCode = ""
    var fileTemplateCode = ""
    var fileSubTemplateCode = ""


    //
    //
    //
    if ("DBDB".equalsIgnoreCase(srcsinkType)) {
      fileTemplateCode = "props/DBDB.template"
      fileSubTemplateCode = "props/DBDB.subtemplate"
    } else if ("FileFile".equalsIgnoreCase(srcsinkType)) {
      fileTemplateCode = "props/FileFile.template"
      fileSubTemplateCode = "props/FileFile.subtemplate"
    } else if ("DBFile".equalsIgnoreCase(srcsinkType)) {
      fileTemplateCode = "props/DBFile.template"
      fileSubTemplateCode = "props/DBFile.subtemplate"
    } else if ("FileDB".equalsIgnoreCase(srcsinkType)) {
      fileTemplateCode = "props/FileDB.template"
      fileSubTemplateCode = "props/FileDB.subtemplate"
    }


//    var frameHouse: FrameHouse = null
//
//    // Load Settings
//    val fileSettings = Source.fromFile("props/db_db_pipeline.settings").getLines.mkString("\n")
//    //println(fileSettings)
//    val settingsTree = tb.parse(fileSettings)
//    val compiledCode = tb.compile(settingsTree)
//    frameHouse = compiledCode().asInstanceOf[FrameHouse]
//    println(frameHouse.getBatchSize())
//    println("++++++++++++++++++")

//    println("Employee dynamic class is getting parsed...")
//    val sourceClass = tb.eval(
//      tb.parse(
//        s"""
//           | package com.aslick.strm.sub
//           |
//           | case class Employee(_id: Int, _name: String) extends com.aslick.strm.BoxTrait with com.aslick.strm.WithTrait {
//           |    def id = _id
//           |    def name = _name
//           | }
//           | scala.reflect.classTag[Employee].runtimeClass
//       """.stripMargin))
//      .asInstanceOf[Class[_]]
//    println("Employee dynamic class is loaded.")
//
//    println("Dynamic Employee class name: " + sourceClass.getName)



    //val fileContents = Source.fromFile("props/DBDB.template").getLines.mkString("\n")
    //println(fileContents)
    val fileContents = getFileTemplateContents(fileTemplateCode, fileSubTemplateCode)
    val clz = tb.eval(tb.parse(fileContents)).asInstanceOf[Class[_]]
    val ctor = clz.getDeclaredConstructors()(0)
    val instance: FrameHouseExecutor = ctor.newInstance(frameHouse).asInstanceOf[FrameHouseExecutor]
    instance.execute()

    println("++++++++++++++++++")

  }

  /**
    * Main Building Factory entry point
    * @param args
    */
  def main_switch_pipeline(args: Array[String]): Unit = {
    val toolbox = tb

    println("FrameController dynamic class is getting parsed...")

    val srcsinkType = s"${frameHouse.getSourceType}${frameHouse.getSinkType}"

    var frameControllerCode = ""

    //
    //
    //
    if ("DBDB".equalsIgnoreCase(srcsinkType)) {
      //
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

      frameControllerCode =
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
         """.stripMargin

    } else if ("FileFile".equalsIgnoreCase(srcsinkType)) {
      //
      val srcCaseString = frameHouse.getSourceFields()
      val srcFileName = frameHouse.getSourceReader()
      val srcFmtClsFields = formatClassFields(srcCaseString, "SourceFrame")
      //println(srcFmtClsFields)
      val srcMapStr = "name:FIRST_NAME,val:FIRST_NAME"
      //val srcMapStr = frameHouse.getSourceMap()
      val srcMap = srcMapStr.split(",").map(f => f.split(":")).map(t => t(0) -> t(1)).toMap
      srcMap.foreach(println)
      val srcFlow = frameHouse.getSourceFlow()
      val srcMapFieldsStr = frameHouse.mapSourceFields()
      val flowGroupSize = frameHouse.getBatchSize()
      val flowAsyncCount = frameHouse.getParallelismCount()
      val sinkCaseString = frameHouse.getSinkFields()
      val sinkFileName = frameHouse.getSinkWriter()
      val sinkFmtClsFields = formatClassFields(sinkCaseString, "SinkFrame")
      val sinkDelete = frameHouse.isSinkDelete()
      val sinkDeleteString = frameHouse.getSinkDelete()
      val sinkCreate = frameHouse.isSinkCreate()
      val sinkCreateStringRaw = frameHouse.getSinkCreate()
      var sinkCreateString = sinkCreateStringRaw
      if(sinkCreateStringRaw.isEmpty)
        sinkCreateString = s"create table ${sinkFileName}(${sinkCaseString})"
      val sinkMapHeaderString = frameHouse.mapSinkHeader()
      val sinkMapSourceString = frameHouse.mapSourceSinkFields()

      //
      val frameControllerCode1 =
        s"""
           |
           |  import com.aslick.strm._
           |  import com.aslick.strm.DynamicLauncher._
           |  import com.aslick.strm.Model.JValueOps
           |  import com.aslick.strm.BaseCryptoCodec.{protect, unprotect}
           |  import com.aslick.strm.Model.{jsonCompactRender, jsonProtect, jsonUnprotect, xmlProtect, xmlUnprotect}
           |  import org.slf4j.LoggerFactory
           |
           |  import java.io.File
           |  import java.nio.file._
           |  import java.time.LocalDateTime
           |
           |  import akka.Done
           |  import akka.actor.ActorSystem
           |  import akka.stream.alpakka.slick.javadsl.SlickSession
           |  import akka.stream.alpakka.slick.scaladsl._
           |  import akka.stream.scaladsl._
           |  import akka.stream.IOResult
           |  import akka.util.ByteString
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
           |  import akka.stream.alpakka.csv.scaladsl.{CsvParsing, CsvToMap, CsvFormatting, CsvQuotingStyle}
           |  import java.nio.charset.{Charset, StandardCharsets}
           |
           |
           |  object FrameController {
           |
           |    val logger = LoggerFactory.getLogger(getClass)
           |
           |    case class SourceFrame(${srcCaseString})
           |
           |    def mapSourceColumns(m: Map[String, String]): SourceFrame = {
           |      ${srcMapFieldsStr}
           |    }
           |
           |    def mapSinkHeader(h: List[String]): String = {
           |      ${sinkMapHeaderString}
           |    }
           |
           |    def mapSinkColumns(s: SourceFrame): String = {
           |      ${sinkMapSourceString}
           |    }
           |
           |
           |    def main(args: Array[String]): Unit = {
           |      //
           |      println("FrameController::main() is invoked")
           |      println(LocalDateTime.now().toString)
           |
           |      //val foreach: Future[IOResult] = FileIO.fromPath(Paths.get("input/cities.csv")).to(Sink.ignore).run()
           |
           |      //val kvMap:  Map[String, String] = stringToMap(s"$srcMapStr")
           |
           |
           |      val done2: Future[IOResult] =
           |        FileIO.fromPath(Paths.get(s"$srcFileName"))
           |          .via($srcFlow)
           |          .map(_.map(_.utf8String))
           |          .zipWithIndex
           |          .filter(_._2 == 0)
           |          .map(t => mapSinkHeader(t._1))
           |          .map(ByteString(_))
           |          .runWith(FileIO.toPath(Paths.get(s"$sinkFileName"), Set(StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE)))
           |
           |      //
           |      done2.failed.foreach(exception => logger.error("failure", exception))
           |      done2.onComplete(_ => {
           |        println(LocalDateTime.now().toString)
           |
           |        //
           |        val done1: Future[IOResult] =
           |          //Source.single(ByteString("eins,zwei,drei\\n"))
           |          FileIO.fromPath(Paths.get(s"$srcFileName"))
           |            .via($srcFlow)
           |            //.via(CsvParsing.lineScanner())
           |            .via(CsvToMap.toMapAsStrings(StandardCharsets.UTF_8))
           |            //.map(_.map {case (key, value) => (key.trim.replaceAll("\\"","").toLowerCase, value)})
           |            .map(m => mapSourceColumns(m))
           |            //.map(_.map { case (k,v) => (k, v.utf8String) }) //.map(_.mapValues(_.utf8String.toString))
           |            //.via(CsvToMap.toMapAsStringsCombineAll(headerPlaceholder = Option.empty))
           |            //.map(_.map(_.utf8String))
           |            //.map(_.map {
           |            //  case (k, v) if (kvMap.contains(k)) => (k, protect(v, kvMap.get(k).get))
           |            //  case x => x
           |            //} )
           |            .map(s => "\\n" + mapSinkColumns(s))
           |            .map(t => ByteString(t))
           |            .runWith(FileIO.toPath(Paths.get(s"$sinkFileName"), Set(StandardOpenOption.APPEND)))
           |            //.runWith(Sink.foreach(println))
           |
           |            //if ($srcMap.contains(k))   protect(v, $srcMap.get(k).get)
           |
           |            done1.failed.foreach(exception => logger.error("failure", exception))
           |            done1.onComplete(_ => {
           |              println(LocalDateTime.now().toString)
           |            })
           |
           |      })
           |
           |
           |      terminationWait(10.seconds)
           |      println(LocalDateTime.now().toString)
           |
           |    }
           |
           | }
           | //scala.reflect.classTag[FrameController].runtimeClass
           |
           | FrameController.main(Array())
           |
         """.stripMargin

      frameControllerCode = getFileTemplateContents("props/FileFile.template", "props/FileFile.subtemplate")

    }

    //
    // Prepare Dynamic Frame Controller class
    //
    val frameControllerClass = toolbox.compile(tb.parse(frameControllerCode))

    //
    frameControllerClass()

    println ("FrameController dynamic class is loaded.")

  }


  /**
    * Main Building Factory entry point
    * @param args
    */
  def main_camelized_keys(args: Array[String]): Unit = {
    val fileContents = Source.fromFile("props/conv_dtls_101.json").getLines.mkString("\n")
    println(fileContents)
    val jsonval = parse(fileContents)
    println(jsonval)
    val jsoncamval = jsonval.snakizeKeys
    println(jsoncamval)

    // FileWriter
    val file = new File("props/conv_dtls_snakized_101.json")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(pretty(render(jsoncamval)))
    bw.close()
  }


  /**
    * Main Building Factory entry point
    * @param args
    */
  def main_conv_id_camelized_keys(args: Array[String]): Unit = {
    val fileContents = Source.fromFile("props/conv_id_101.json").getLines.mkString("\n")
    println(fileContents)
    val jsonval = parse(fileContents)
    println(jsonval)
    val jsoncamval = jsonval.snakizeKeys
    println(jsoncamval)

    // FileWriter
    val file = new File("props/conv_id_snakized_101.json")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(pretty(render(jsoncamval)))
    bw.close()
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
      case class User(id: Int, name: String)
      val u = User(42, "Joe")
      val list = u.productIterator.toList.map(_.toString).toList
      list.foreach(println)
      val clsFlds = classOf[User].getDeclaredFields.map(_.getName).toList
      clsFlds.foreach(println)


      //main_conv_id_camelized_keys(args)
      //main_camelized_keys(args)
      //main_pipeline(args)
      //main_json(args)
      //main_conv_agg(args)
      //main_eval_hello(args)
      main_template_pipeline(args)
      //main_switch_pipeline(args)
      //main_file_util(args)

      //val keysmap = Map("City" -> "FIRST_NAME", "State" -> "FIRST_NAME")
      //keysmap.map(t => println(t))

      //val csvfields = "LatD:1,LatM:2,LatS:3,City:Tampa,State:FL"
      //val csvcols = csvfields.split(",").map(f => f.split(":")).map(t => t(0) -> t(1)).toMap

      //val myfields = "City:FIRST_NAME,State:FIRST_NAME"
      //val hlistcols = myfields.split(",").map(f => f.split(":")).map(t => t(0) -> t(1)).toMap
        //.map(a => a.map(case Array(k, v) => k -> v) )
      //hlistcols.map(t => println(t))
        //.map(f => f.split(":")).map(a => { case Array(k, v) => k -> v }.toMap)

      //val replaced = csvcols.map {
      //  case (k, v) if (hlistcols.contains(k)) => (k, BaseCryptoCodec.protect(v, hlistcols.get(k).get))
      //  case x => x
      //}
      //replaced.map(t => println(t))

      //val str = "NAME=bala AGE=23 COUNTRY=Singapore"
      //val pairs = str.split("=| ").grouped(2)
      //val map = pairs.map { case Array(k, v) => k -> v }.toMap
      //map.foreach(t => println(t))
    } catch {
      case e: Exception => {println(e.printStackTrace())}
      case t: Throwable => println("Failed..." + t.printStackTrace())
    }

    //
    terminateActorSystem()
  }

}
