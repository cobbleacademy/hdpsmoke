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
    def getBatchSize(): Int
    def getParallelismCount(): Int
    def getSinkType(): String
    def getSinkFields(): String
    def isSinkDelete(): Boolean
    def getSinkDelete(): String
    def isSinkCreate(): Boolean
    def getSinkCreate(): String
    def getSinkWriter(): String
    def mapSourceSinkFields(): String
    def getJsonSyntax(): String
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


}
