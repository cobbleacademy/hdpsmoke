package com.drake.reader

import com.drake.{BaseTrait, SparkHelper}
import com.drake.schema.SchemaBuilderHelper
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.json4s.DefaultFormats
import org.json4s.Xml.{toJson, toXml}
import org.json4s.jackson.JsonMethods.{compact, render, parse}

import scala.collection.mutable.ListBuffer
import scala.xml.XML

/**
  * A Helper class to facilitate reading XML multiline tags
  */
object WholeFileXMLReaderHelper extends BaseTrait {


  /**
    * Creates a DataFrame from incoming multi line XML documents
    * @param inputRDD
    * @param rootTag
    * @return
    */
  def createWholeFileXMLDataFrame(inputRDD: RDD[(String, String)], rootTag: String): DataFrame = {

    //
    val rddSchema = SchemaBuilderHelper.createWholeFileTextSchema()
    val ss = SparkHelper.getSparkSession()

    //
    val jsonStringRDD = inputRDD.flatMap(item => {

      val key = item._1
      val keyvalue = item._2

      //
      val lookupElement = rootTag
      val beginTag = "<" + lookupElement
      val endTag = "</"+lookupElement+">"
      val elemArray = ListBuffer[String]();

      var ctgInd = -1
      ctgInd = keyvalue.indexOf(beginTag, ctgInd)
      while (ctgInd != -1) {
        //
        implicit val formats = DefaultFormats

        val toCtgInd = keyvalue.indexOf(endTag, ctgInd) + endTag.length
        val currElemData = keyvalue.substring(ctgInd, toCtgInd)
        val jsonStr = compact(render(toJson(XML.loadString("<tojson>"+currElemData+"</tojson>").child)))
        elemArray += jsonStr
        ctgInd = keyvalue.indexOf(beginTag, toCtgInd)
      }

      elemArray.toArray[String]
    })

    //
    val jsonRDD = jsonStringRDD.map(item => Row(item))

    //
    val jsonDF: DataFrame = ss.createDataFrame(jsonRDD, rddSchema)


    //
    jsonDF
  }

}
