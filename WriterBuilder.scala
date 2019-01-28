package com.drake.writer

import java.util.UUID

import com.drake.model.Model.Step
import com.drake.schema.SchemaBuilderHelper
import com.drake.status.{StatusBuilder, StatusBuilderHelper}
import com.drake.{BaseTrait, PropsUtil, SessionDataHelper, SparkHelper}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions.{current_timestamp, lit, udf}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SaveMode, functions}

import scala.collection.mutable

/**
  * A Builder class for Writer for output content
  */
object WriterBuilder {

  /**
    * Builds specific Writer
    */
  class BaseWriter(handlername: String) extends WriterBuilder {

    name = handlername

    val generateUUID = udf(() => UUID.randomUUID().toString)

    /**
      * Writes DataFrame content to Sink
      *
      * @param step
      * @return
      */
    override def writeSink(step: Step, outputFrame: DataFrame): Unit = {
      logger.debug("BaseReader:readSource")

      //
      //val sinkOpts = step.options.getOrElse(Seq(Attribute("","")))
      //val sinkOptsMap = Map(sinkOpts map {a => a.key -> a.value}: _*)
      //val sinkAttrs = step.attributes.getOrElse(Seq(Attribute("","")))
      //val sinkAttrsMap = Map(sinkAttrs map {a => a.key -> a.value}: _*)
      val sinkOpts = step.options.getOrElse(Map())
      val sinkAttrs = step.attributes.getOrElse(Map())
      val sparkSession = SparkHelper.getSparkSession()


      //
      if ("stream".equals(PropsUtil.getWorkflow().mode)) {

        //
        if (sinkAttrs.get("partitionBy").isDefined) {

          val partCols = sinkAttrs.getOrElse("partitionBy", "").split(",")
          //
          val outQuery = outputFrame
            .withColumn("uuid", generateUUID())
            .writeStream
            .format(step.format.get)
            .options(sinkOpts)
            .partitionBy(partCols: _*)
            .trigger(Trigger.ProcessingTime(sinkAttrs.getOrElse("trigger", "")))
            .start()

          //
          //outQuery.awaitTermination()

        } else {

          //
          val outQuery = outputFrame
            .withColumn("uuid", generateUUID())
            .writeStream
            .format(step.format.get)
            .options(sinkOpts)
            .trigger(Trigger.ProcessingTime(sinkAttrs.getOrElse("trigger", "")))
            .start()

          //
          //outQuery.awaitTermination()

        }
      } else if ("batch".equals(PropsUtil.getWorkflow().mode)) {

        //
        val sqlHive = sinkAttrs.getOrElse("sql", "").trim
        //
        if ("hbase".equals(step.format.getOrElse(""))) {

          //
          val outQuery = outputFrame
            .write
            .format("org.apache.spark.sql.execution.datasources.hbase")
            .option(HBaseTableCatalog.tableCatalog, SchemaBuilderHelper.createCatalog(sinkAttrs))
            .option(HBaseTableCatalog.newTable, "5")
            .save()

        } else if ("hive".equals(step.format.getOrElse("")) && sqlHive.length > 0) {
          //
          outputFrame.createOrReplaceTempView(sinkAttrs.getOrElse("currentTempView", ""))
          sparkSession.sql(sqlHive)

        } else {
          //
          val bucketNumEval = sinkAttrs.getOrElse("numBuckets", "0").toInt
          //
          val bucketByEval = sinkAttrs.get("bucketBy").fold(Array[String]()){_.split(",")}
          //
          val sortyByEval = sinkAttrs.get("sortBy").fold(Array[String]()){_.split(",")}
          //
          val partColsEval = sinkAttrs.get("partitionBy").fold(Array[String]()){_.split(",")}
          //
          val saveModeEval = sinkAttrs.getOrElse("saveMode", "Append")
          val saveMode: SaveMode = SaveMode.valueOf(saveModeEval)
          //
          val saveAsTable = sinkAttrs.getOrElse("saveAsTable", "")
          println("bucketNumEval: " + bucketNumEval)
          println("bucketByEvaL: " + bucketByEval + " size: " + bucketByEval.size)
          println("sortyByEval: " + sortyByEval + " size: " + sortyByEval.size)
          println("partColsEval: " + partColsEval + " size: " + partColsEval.size)
          println("saveModeEval: " + saveModeEval)

          outputFrame.printSchema()


          //
          val formatWriter = outputFrame
            .write //Stream
            .options(sinkOpts)
            .format(step.format.get)
//            .format("hive")//step.format.get)
//            .partitionBy(partColsEval: _*)
//            .mode(SaveMode.Append)
//            .saveAsTable("default.sales_dom")
          //
          val bucketWriter =
            if (bucketNumEval > 0)
              if (bucketByEval.size == 1)
                formatWriter.bucketBy(bucketNumEval, bucketByEval(0))
              else
                formatWriter.bucketBy(bucketNumEval, bucketByEval(0), bucketByEval.drop(1): _*)
            else
              formatWriter
          //
          val sortWriter =
            if (sortyByEval.size > 0)
              if (sortyByEval.size == 1)
                bucketWriter.sortBy(sortyByEval(0))
              else
                bucketWriter.sortBy(sortyByEval(0), sortyByEval.drop(1): _*)
            else
                bucketWriter
          //
          val partWriter =
            if (partColsEval.size > 0)
              sortWriter.partitionBy(partColsEval: _*)
            else
              sortWriter
          //
          val modeWriter = partWriter.mode(saveMode)
          //
          if (!saveAsTable.isEmpty)
            modeWriter.insertInto(saveAsTable)//.saveAsTable(saveAsTable)
          else
            modeWriter.save()


        }

        //Refresh Hive Meta Info
        if ("true".equals(sinkAttrs.getOrElse("hiveMetaRepair", "false"))) {
          outputFrame.sparkSession.sql("msck repair table " + sinkAttrs.getOrElse("db", "") + "." + sinkAttrs.getOrElse("table", ""))
          //output.sparkSession.sql("select * from " + sinkAttrs.getOrElse("db", "") + "." + sinkAttrs.getOrElse("table", "")).show(false)
        }


//        //
//        if ("hive".equals(step.format.getOrElse(""))) {
//          //
//          outputFrame.createOrReplaceTempView(sinkAttrs.getOrElse("currentTempView", ""))
//          sparkSession.sql(sinkAttrs.getOrElse("sql", ""))
//          outputFrame.write
//            //bucketBy(null,null)
//
//        } else {
//
//          //
//          if (sinkAttrs.get("partitionBy").isDefined) {
//            //
//            val partCols = sinkAttrs.getOrElse("partitionBy", "").split(",")
//            //
//            val outQuery = outputFrame
//              //.withColumn("uuid", generateUUID())
//              .write //Stream
//              .format(step.format.get)
//              .options(sinkOpts)
//              .partitionBy(partCols: _*)
//              .mode(SaveMode.Append)
//              .save(sinkOpts.getOrElse("path", ""))
//            //.trigger(Trigger.ProcessingTime(sinkAttrs.getOrElse("trigger",""))
//            //.start()
//
//            //
//            //outQuery.awaitTermination()
//
//          } else if ("hbase".equals(step.format.getOrElse(""))) {
//
//            //
//            val outQuery = outputFrame
//              .write
//              .format("org.apache.spark.sql.execution.datasources.hbase")
//              .option(HBaseTableCatalog.tableCatalog, SchemaBuilderHelper.createCatalog(sinkAttrs))
//              .option(HBaseTableCatalog.newTable, "5")
//              .save()
//
//            //
//            //outQuery.awaitTermination()
//
//          } else {
//            //
//            val outQuery = outputFrame
//              //.withColumn("uuid", generateUUID())
//              .write //Stream
//              .format(step.format.get)
//              .options(sinkOpts)
//              .mode(SaveMode.Append)
//              .save()
//              //.save(sinkOpts.getOrElse("path", ""))
//            //.trigger(Trigger.ProcessingTime(sinkAttrs.getOrElse("trigger",""))
//            //.start()
//
//            //
//            //outQuery.awaitTermination()
//
//          }
//
//          //Refresh Hive Meta Info
//          if ("true".equals(sinkAttrs.getOrElse("hiveMetaRepair", "false"))) {
//            outputFrame.sparkSession.sql("msck repair table " + sinkAttrs.getOrElse("db", "") + "." + sinkAttrs.getOrElse("table", ""))
//            //output.sparkSession.sql("select * from " + sinkAttrs.getOrElse("db", "") + "." + sinkAttrs.getOrElse("table", "")).show(false)
//          }
//
//        }

        // Run post process scripts
        if ("true".equals(sinkAttrs.getOrElse("logStatus", "false"))) StatusBuilderHelper.endLogStatus(step, sinkAttrs.getOrElse("logStatusFrom", ""))

      }

    }


  }

  /**
    * preferred factory method
    *
    * @param s
    * @return
    */
  def apply(s: String): WriterBuilder = {
    getWriterBuilder(s)
  }

  // an alternative factory method (use one or the other)
  def getWriterBuilder(s: String): WriterBuilder = {
    new BaseWriter(s)
  }

}

/**
  * A Builder interface for all the Sink Writer
  */
trait WriterBuilder extends BaseTrait {

  var name: String = _


  /**
    * Writes DataFrame content to Sink
    *
    * @param step
    * @return
    */
  def writeSink(step: Step, output: DataFrame): Unit

}
