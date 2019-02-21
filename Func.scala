package com.drake.function

import java.util.UUID

import com.drake.BaseTrait
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.Xml._


import scala.xml._


/**
  * A common functions needed for processing data
  */
object Func extends BaseTrait {

  implicit val formats = DefaultFormats

  /**
    * Define scala function
    */
  val xmltojsonfunc: String => String = (input: String) => compact(render(toJson(XML.loadString("<tojson>"+input+"</tojson>").child)))

  /**
    * Generates glbal UUID
    */
  val uuid = udf(() => UUID.randomUUID().toString)


  /**
    * Defines spark udf from scala function
    */
  val xmltojson = udf(xmltojsonfunc)


  /**
    * Genertes Global UUID
    * @param dataframe
    * @return
    */
  def registerUUID(dataframe: DataFrame) = {
    dataframe.sparkSession.sqlContext.udf.register("uuid", () => UUID.randomUUID().toString)
  }

  /**
    * Genertes Global UUID
    * @param dataframe
    * @return
    */
  def registerXmlToJson(dataframe: DataFrame) = {
    dataframe.sparkSession.sqlContext.udf.register("xmltojson", () => xmltojson)
  }


  /**
    * Registers all functions
    * @param dataframe
    * @return
    */
  def registerFunctions(dataframe: DataFrame) = {
    registerUUID(dataframe: DataFrame)
    registerXmlToJson(dataframe: DataFrame)

  }

}
