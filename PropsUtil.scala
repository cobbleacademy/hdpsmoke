package com.akkarest.service

import java.util.Properties

import com.akkarest.service.Models.PatternRequest

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Try

//import org.json4s.DefaultFormats
//import org.json4s.jackson.JsonMethods.parse

/**
  * Reads property file to provide key based values
  */
object PropsUtil {

  private val props = new Properties()
  private var patterns: Map[String, String] = Map[String, String]()
  private var patternQueries: Map[String, String] = Map[String, String]()

  private val PATTERN_REQ_PFX = "mem.pattern.req."
  private val PATTERN_QUERY_PFX = "mem.pattern.query."

  //implicit val formats = DefaultFormats
  /**
    * Load properties using property file
    */
  def loadProperties(): Unit = {
    if (props.size() == 0) {
      //val path = System.getProperty("propertyFile")
      val path = "/Users/nnagaraju/IDEAWorkspace/akkarset/src/main/resources/memberservice.properties"
      println("loading properties file from " + path)
      val bufferedFile = Source.fromFile(path)
      props.load(bufferedFile.reader())
      bufferedFile.close()
      println("logging all kehy values from properties file " + props.entrySet())
    }
  }


  /**
    * Get the value for given key
    *
    * @param key
    * @return
    */
  def propValue(key: String) = {
    props.getProperty(key)
  }

  /**
    * Get the Long value for given key
    *
    * @param key
    * @return
    */
  def propLong(key: String) = {
    props.getProperty(key).toLong
  }

  /**
    * Get the Boolean value for given key
    *
    * @param key
    * @return
    */
  def propBoolean(key: String) = {
    Try(props.getProperty(key).toLowerCase.toBoolean).getOrElse(false)
  }


  /**
    * Returns member patterns
    * @param begin
    */
  def loadMemPatterns(begin: String): Unit = {

    //"mem.pattern.req."

    // convert Java Properties to Scala Map
    import scala.collection.JavaConverters._
    val scalaProps = props.asScala
    scalaProps
      .filter(f => f._1.startsWith(begin))
      .foreach(f => {
        patterns += (f._1 -> f._2)
      })
    patterns.foreach(println)

  }

  /**
    * Returns member patterns
    * @param begin
    */
  def loadMemPatternQueries(begin: String): Unit = {

    //"mem.pattern.query."

    // convert Java Properties to Scala Map
    import scala.collection.JavaConverters._
    val scalaProps = props.asScala
    scalaProps
      .filter(f => f._1.startsWith(begin))
      .foreach(f => {
        patternQueries += (f._1 -> f._2)
      })
    patternQueries.foreach(println)

  }

  /**
    * initialize with properties and workflow
    */
  def initialize(): Unit = {
    loadProperties()
    loadMemPatterns(PATTERN_REQ_PFX)//"mem.pattern.req.")
    loadMemPatternQueries(PATTERN_QUERY_PFX)//"mem.pattern.query.")
  }


  /**
    * Retruns MemPattern number
    * @param req
    * @return
    */
  def findMemPattern(req: PatternRequest): String = {

    //
    var includedBuf = new ListBuffer[String]()
    var excludedBuf = new ListBuffer[String]()

    //
    if (req.pk.isEmpty) excludedBuf += "pk" else includedBuf += "pk"
    if (req.name.isEmpty) excludedBuf += "name" else includedBuf += "name"

    println("included: " + includedBuf)
    println("excluded: " + excludedBuf)

    if (includedBuf.size > 0) {
      //println("inside included")
      patterns.foreach(f => {
        //println("inside tobezero s" + f._1)
        val filtnot = f._2.split(",").toSet
        val outzero = filtnot.filterNot(includedBuf.toList.toSet)
        val inzero = includedBuf.toList.filterNot(filtnot)
        if( inzero.size == 0 && outzero.size == 0 ) {
          val li = f._1.substring(f._1.lastIndexOf(".")+1)
          val qry = patternQueries.get(PATTERN_QUERY_PFX+li).getOrElse("NOT FOUND")
          println("pattern seq for included: " + li)
          println("pattern query for included: " + qry)
        }
      })
    }


    ""
  }


}
