package com.drake

import java.io.{File, FileInputStream, InputStreamReader}
import java.util.Properties

import com.drake.model.Model.Workflow
import com.drake.offset.OffsetStore
import org.apache.kafka.common.serialization.StringDeserializer
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import io.circe.yaml.parser
import org.apache.spark.SparkFiles

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Try


/**
  * Reads property file to provide key based values
  */
object PropsUtil {

  private val props = new Properties()
  private var workflow: Workflow = _

  implicit val formats = DefaultFormats

  /**
    * Load properties using property file
    */
  def loadProperties(): Unit = {
    if (props.size() == 0) {
      val path = System.getProperty("propertyFile")
      //val path = "/Users/nnagaraju/IDEAWorkspace/drake/src/main/resources/sparkpipeline.properties"
      println("loading properties file from " + path)
      val bufferedFile = Source.fromFile(path)
      props.load(bufferedFile.reader())
      bufferedFile.close()
      println("logging all kehy values from properties file " + props.entrySet())
    }
  }

  /**
    * Load json based pipeline from file
    */
  def loadWorkflow(): Unit = {
    //
    val path = System.getProperty("workflowFile")
    //val path = "/Users/nnagaraju/IDEAWorkspace/drake/src/main/resources/workflow.json"
    val bufferedFile = Source.fromFile(path)
    println("printing json contents")
    val json = parse(bufferedFile.getLines.mkString)
    println(json)
    workflow = json.extract[Workflow]
    println(workflow)
    bufferedFile.close()
  }


  /**
    * Load json based pipeline from file
    */
  def loadWorkflowYaml(): Unit = {
    //
    val path = System.getProperty("workflowFileYaml")
    //val path = "/Users/nnagaraju/IDEAWorkspace/drake/src/main/resources/workflow_standalone.yml"
    //val bufferedFile = Source.fromFile(path)
    //val json = parser.parse(bufferedFile.getLines.mkString("\n"))
    //bufferedFile.close()
    println("printing yaml contents")
    val fstream = new FileInputStream(new File(path))
    val jsonObj = io.circe.yaml.parser.parse(new InputStreamReader(fstream))
    println(jsonObj)
    val json = parse(jsonObj.right.get.toString)
    workflow = json.extract[Workflow]
    println(workflow)
    fstream.close()
  }


  /**
    * initialize with properties and workflow
    */
  def initialize(): Unit = {
    loadProperties()
    //loadWorkflow()
    loadWorkflowYaml()
  }


  /**
    * Returns workflow of events
    * @return
    */
  def getWorkflow(): Workflow = {
    workflow
  }


  /**
    * Returns scala code from path file to create schema
    * @param path
    * @return
    */
  def loadSchemaCode(path: String): String = {
    val bufferedFile = Source.fromFile(path)
    //println("printing SchemaCode contents")
    val code = bufferedFile.getLines.mkString("\n")
    //println(code)
    code
  }

  /**
    * Returns scala code from path file to create schema
    * @param path
    * @return
    */
  def loadContainsSchemaCode(path: String): String = {
    val bufferedFile = Source.fromFile(path)
    var code = ""
    var startAdd = false
    var stopAdd = false
    var codeArray: ListBuffer[String] = ListBuffer[String]()
    bufferedFile.getLines.foreach(f => {
      var clf = f
      if(startAdd && f.contains("containsSchemaVal")) clf = f.split(" = ")(1)
      if (f.contains("END")) stopAdd = true
      if (startAdd && !stopAdd) codeArray += clf
      if (f.contains("BEGIN")) startAdd = true
    })
    code = codeArray.mkString("\n")
    //println(code)
    code
  }

  /**
    * Get DStream based Kafka Parameters
    *
    * @return
    */
  def getKafkaParams(): Map[String, Object] = {
    val kafkaParams = Map("group.id" -> propValue("kafka.group.id"),
      "security.protocol" -> propValue("kafka.security.protocol"),
      "bootstrap.servers" -> propValue("kafka.metadata.broker.list"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "none",
      "heartbeat.interval.ms" -> (100000: java.lang.Integer),
      "session.timeout.ms" -> (300000: java.lang.Integer),
      "fetch.max.wait.ms" -> (100000: java.lang.Integer),
      "request.timeout.ms" -> (400000: java.lang.Integer),
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    kafkaParams
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
    * Get Kafka Source Connection parameters
    *
    * @return
    */
  def getKafkaConsumerParams(): Map[String, String] = {
    //
    val removePrefix = props.getProperty("kafka.nonprefixkeys","").split(",")

    // return map of spark params
    props.entrySet().filter(x => x.getKey.toString.startsWith("kafka.")).map(x => {
      var key = x.getKey.toString
      var value = x.getValue.toString
      if (removePrefix.contains(key.split("\\.")(1))) key = key.split("\\.")(1)
      if (key.equals("startingOffsets")) value = OffsetStore.getOffsetStore.readOffsets(Seq[String](propValue("kafka.subscribe").mkString(",")),propValue("kafka.group-id"),value)

      (key -> value)
    }).toMap
  }

  /**
    * Get map of spark parameters
    *
    * @return
    */
  def getSparkParams(): Map[String, String] = {

    //return map of spark params
    props.entrySet().filter(x => x.getKey.toString.startsWith("spark.")).map(x => {
      (x.getKey.toString -> x.getValue.toString)
    }).toMap
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


}
