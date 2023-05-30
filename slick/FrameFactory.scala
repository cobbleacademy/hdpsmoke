package com.aslick.strm.factory

import scala.io.Source
import scala.tools.reflect.ToolBox
import scala.reflect.runtime.universe

object FrameFactory {

  /**
    * An abstract FrameHouse class
    */
  abstract class FrameHouse {
    def getSourceType(): String
    def getSourceFields(): String
    def getSourceReader(): String
    def getSourceTableFormat(): String = {formatClassFields(getSourceFields(), "SourceFrame")}
    def getSourceFlow(): String = {""}
    def mapSourceFields(): String = {""}
    def getBatchSize(): Int = {256}
    def getParallelismCount(): Int = {1}
    def getSinkType(): String
    def getSinkFields(): String
    def isSinkDelete(): Boolean = {true}
    def getSinkDelete(): String = {""}
    def isSinkCreate(): Boolean = {true}
    def getSinkCreate(): String = {createTableSyntax(getSinkWriter(), getSinkFields())}
    def getSinkWriter(): String
    def getSinkTableFormat(): String = {formatClassFields(getSinkFields(), "SinkFrame")}
    def mapSinkHeader(): String = {""}
    def mapSourceSinkFields(): String
    def getJsonSyntax(): String = {""" FRAME_SINK """}

    /**
      * Returns create table syntax
      * @param tableName
      * @param fields
      * @return
      */
    def createTableSyntax(tableName: String, fields: String): String = {
      s"create table $tableName($fields)"
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

    /**
      * Returns a sample json value
      * @param ind
      * @return
      */
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


    /**
      * Returns a sample xml value
      * @param ind
      * @return
      */
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

  }

  // global initialized variables
  var frameHouse: FrameHouse = _

  /**
    * A Factory class to build different classes
    * @param implFilename
    */
  case class FrameHouseFactory(implFilename: String, tb: ToolBox[universe.type]) {

    val fileContents = Source.fromFile(implFilename).getLines.mkString("\n")
    //println(fileContents)
    val tree = tb.parse("import com.aslick.strm.factory.FrameFactory._ " + "\n" + fileContents)
    val compiledCode = tb.compile(tree)

    //    val eval = new Eval()
    //    val framehouse = eval.apply[FrameHouse](new File("jdbc_pipeline.properties"))


    def make(): FrameHouse = {
      frameHouse = compiledCode().asInstanceOf[FrameHouse]
      //
      frameHouse
    }
  }


  /**
    * Returns FrameHouse instance
    * @return
    */
  def getFrameHouse(): FrameHouse = {
    frameHouse
  }


  /**
    * Initialize Factory
    * @param tb
    */
  def initialize(implFilename: String, tb: ToolBox[universe.type]): Unit = {
    FrameHouseFactory(implFilename, tb).make()
  }


  /**
    * An abstract Executor pattern class
    */
  abstract class FrameHouseExecutor(frameHouse: FrameHouse) {

    /**
      * Executes the pipeline
      */
    def execute()

  }




}
