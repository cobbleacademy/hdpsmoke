package com.drake.reader

import java.text.SimpleDateFormat
import java.util.Calendar

import com.drake._
import com.drake.model.Model.Step
import com.drake.offset.OffsetStore
import com.drake.schema.SchemaBuilderHelper
import com.drake.status.{StatusBuilder, StatusBuilderHelper}
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{current_timestamp, lit}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.execution.datasources.hbase._

import scala.collection.mutable

/**
  * A Builder interface for all the Source Readers
  */
object ReaderBuilder {

  /**
    * Builds specific Reader
    */
  class BaseReader(handlername: String) extends ReaderBuilder {

    name = handlername

    /**
      * Saves Properties in SessionData
      *
      * @param step
      * @param readFile
      */
    def saveSessionData(step: Step, readFrame: DataFrame): Unit = {
      //
      val stepAttrs = step.attributes.getOrElse(Map())
      val postAttrs = step.post.getOrElse(Map())

      //
      val cnt = if (readFrame.isStreaming) -1 else readFrame.rdd.count()
      val fnames = if (readFrame.isStreaming) "" else readFrame.inputFiles.map(_.split("/").last).mkString("|")
      val now = Calendar.getInstance().getTime()
      val nowFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SXXX")
      val nowStr = nowFormat.format(now)
      println("Date format: " + nowStr)
      val statusComment = stepAttrs.getOrElse("statusComment", "")
      //
      val sessDataMap = SessionDataHelper.getSessionData()
      val stepMap: mutable.Map[String, String]
      = mutable.Map("fileName" -> fnames, "fileDt" -> "2018-12-22", "fileHr" -> "00", "count" -> cnt.toString, "startTime" -> nowStr, "endTime" -> "", "comment" -> statusComment, "srcSystem" -> SessionDataHelper.parentageLookup("$args#0"))
      sessDataMap += (name -> stepMap)

      // Run post process scripts
      if (!postAttrs.isEmpty) {
        import sys.process._
        val cmd = postAttrs.getOrElse("path", "") + "/" + postAttrs.getOrElse("name", "")
        val shellResult = Process(cmd).lineStream //cmd.!!;val arr = shellResult.split("\\n")
        stepMap.foreach(println)
        println("Printing shellResult....")
        shellResult.foreach(x => {
          val arr = x.split("=")
          println(x + " ++ " + arr(0) + " = " + arr(1))
          stepMap += (arr(0) -> arr(1))
          println(stepMap.getOrElse(arr(0), ""))
        })
        println("Printing shellResult after exec..")
        stepMap.foreach(println)
      }

    }

    /**
      * Returns DataFrame by reading data from source with defined type
      *
      * @param step
      * @param inDataFrame
      * @return
      */
    override def readSource(step: Step, inDataFrame: Option[DataFrame] = None): DataFrame = {
      logger.debug("BaseReader:readSource")

      //
      val sparkSession = SparkHelper.getSparkSession()

      //
      //val stepOpts = step.options.getOrElse(Seq(Attribute("", ""))) val stepOptsMap = Map(stepOpts map { a => a.key -> a.value }: _*)
      //
      val stepOpts = step.options.getOrElse(Map())
      val stepAttrs = step.attributes.getOrElse(Map())
      val postAttrs = step.post.getOrElse(Map())

      // not needed when running on cluster but stanalone mode
      //val localPrefix = "/Users/nnagaraju/IDEAWorkspace/drake/"
      val localPrefix = ""

      //val ldstDrTbl = "drop table if exists load_status"
      //val ldstCrTbl = s"create table if not exists load_status(file_name string,file_dt string,file_hr string,record_count string,start_time string,end_time string,comment string) partitioned by (src_system string) row format delimited fields terminated by ',' stored as textfile location '${localPrefix}output/load_status'"
      //val stgPartDrTbl = "drop table if exists sales_part_stg"
      //val stgPartCrTbl = "create external table if not exists sales_part_stg(transactionId int,customerId int,itemId int,amountPaid double) partitioned by (src_system string,event_dt string) row format delimited fields terminated by ',' stored as textfile location 'input/sales_part_stg'"
      //val stgDrTbl = "drop table if exists sales_stg"
      //val stgCrTbl = "create external table if not exists sales_stg(transactionId int,customerId int,itemId int,amountPaid double) row format delimited fields terminated by ',' stored as textfile location 'input/sales'"
      //val domDrTbl = "drop table if exists sales"
      //val domCrTbl = "create table if not exists sales(transactionId int,customerId int,itemId int,amountPaid double) partitioned by (process_dt string, process_hr int) row format delimited fields terminated by ',' stored as textfile location 'output/sales'"
      //val salesInvDrTbl = "drop table if exists sales_invalid"
      //val salesInvCrTbl = s"create table if not exists sales_invalid(transactionId int,customerId int,itemId int,amountPaid double,invalid_reason string,uuid string,process_epoch bigint,process_dt string,process_hr string) partitioned by (event_dt string, event_hr int) row format delimited fields terminated by ',' stored as textfile location '${localPrefix}output/sales_invalid'"
      //CLUSTERED BY(customerId) INTO 2 BUCKETS
      val domDrTbl = "drop table if exists sales_dom"
      val domCrTbl = s"create table if not exists sales_dom(transactionId int,customerId int,itemId int,amountPaid double,uuid string,process_epoch bigint,process_dt string,process_hr string) partitioned by (event_dt string, event_hr string) row format delimited fields terminated by ',' stored as textfile location '${localPrefix}output/sales_dom'"
      //val domCrTbl1 = s"create table if not exists sales_dom(transactionId int,customerId int,itemId int,amountPaid double,uuid string,process_epoch bigint,process_dt string,process_hr string) partitioned by (event_dt string, event_hr string) row format delimited fields terminated by ',' stored as textfile location '${localPrefix}output/sales_dom'"
      //val invColCfgDrTbl = "drop table if exists invalid_col_config"
      //val invColCfgCrTbl = s"create table if not exists invalid_col_config(table_name string,col_name string,data_type string,data_type_check int,null_check int,empty_check int,dup_check int,range_check int,range_check_values string,const_check int,const_check_values string) partitioned by (src_system string) row format delimited fields terminated by ',' stored as textfile location '${localPrefix}input/config/invalid_col_config'"
      //val invColStDrTbl = "drop table if exists invalid_col_stats"
      //val invColStCrTbl = s"create table if not exists invalid_col_stats(table_name string,col_name string,sum_dtype_chk_cnt bigint,sum_null_chk_cnt bigint,sum_empty_chk_cnt bigint,sum_dup_chk_cnt bigint,sum_range_chk_cnt bigint,sum_const_chk_cnt bigint,event_hr int,process_epoch bigint,process_dt string,process_hr string) partitioned by (src_system string, event_dt string) row format delimited fields terminated by ',' stored as textfile location '${localPrefix}output/invalid_col_stats'"
      //val partIncrDrTbl = "drop table if exists part_incr"
      //val partIncrCrTbl = s"create table if not exists part_incr(partid bigint,partname string,cost double,updated string) partitioned by (event_dt string) row format delimited fields terminated by ',' stored as textfile location '${localPrefix}output/part_incr'"
      //val partDrTbl = "drop table if exists part"
      //val partCrTbl = s"create table if not exists part(partid bigint,partname string,cost double,updated string) partitioned by (event_dt string) row format delimited fields terminated by ',' stored as textfile location '${localPrefix}output/part'"
      //sparkSession.sql(ldstDrTbl)
      //sparkSession.sql(ldstCrTbl)
      //sparkSession.sql(stgPartDrTbl)
      //sparkSession.sql(stgPartCrTbl)
      //sparkSession.sql("msck repair table sales_part_stg")
      //sparkSession.sql(stgDrTbl)
      //sparkSession.sql(stgCrTbl)
      //sparkSession.sql(salesInvDrTbl)
      //sparkSession.sql(salesInvCrTbl)
      sparkSession.sql(domDrTbl)
      sparkSession.sql(domCrTbl)
      //sparkSession.sql(invColCfgDrTbl)
      //sparkSession.sql(invColCfgCrTbl)
      //sparkSession.sql(invColStDrTbl)
      //sparkSession.sql(invColStCrTbl)
      //sparkSession.sql(partIncrDrTbl)
      //sparkSession.sql(partIncrCrTbl)
      //sparkSession.sql(partDrTbl)
      //sparkSession.sql(partCrTbl)

      //
      sparkSession.sql("show databases").show(false)
      sparkSession.sql("show tables").show(false)
      //sparkSession.sql("select * from sales_stg").show(false)
      //sparkSession.sql("show partitions sales").show(false)
      sparkSession.sql("show partitions sales_dom").show(false)
      //sparkSession.sql("alter table sales_dom drop if exists partition (process_dt = '2018-12-22')")
      //sparkSession.sql("show partitions sales_dom").show(false)
      //sparkSession.sql("select * from sales").show(false)
      //sparkSession.sql("select * from sales_dom").show(false)
      //sparkSession.sql("show partitions load_status").show(false)

      //
      var inStreamDf: DataFrame = if (inDataFrame.isDefined) inDataFrame.get else null

      //
      // A pre constructed dataframe or dataframe from Step
      //
      if (inStreamDf == null) {

        //
        if ("stream".equals(PropsUtil.getWorkflow().mode)) {

          //
          var streamDf: DataFrame = null

          //
          if ("kafka".equals(step.format.getOrElse(""))) {

            //
            val offsetStore = OffsetStore
              .builder()
              .config("zkHosts", PropsUtil.propValue("zookeeper.hosts"))
              .config("zkBasePath", PropsUtil.propValue("zookeeper.base.path") + "/" + PropsUtil.propValue("kafka.group-id") + "/offsets/" + stepOpts.getOrElse("subscribe", ""))
              .getOrCreate("zookeeper")

            // merge bot main and workflow options
            val mergedOpts = PropsUtil.getKafkaConsumerParams(stepOpts) ++ stepOpts

            //
            streamDf = sparkSession
              .readStream
              .options(mergedOpts)
              .format(step.format.getOrElse(""))
              .load()
              .selectExpr(stepAttrs.getOrElse("kafkaSelectExpr", "*"))

          } else {

            //
            streamDf = sparkSession
              .readStream
              .options(stepOpts)
              .format(step.format.getOrElse(""))
              .schema(SchemaBuilderHelper.createSchema(stepAttrs)) // create schema
              .load()

          }

          // return
          inStreamDf = streamDf

        } else {

          if ("wholefile".equals(step.format.getOrElse(""))) {

            // get wholefile format
            val wholeFilePath = stepOpts.getOrElse("path", "")
            val wholeFileFormat = stepOpts.getOrElse("fileFormat", "text")
            val wholeFileContent = stepOpts.getOrElse("contentType", "xml")
            val xmlTag = stepOpts.getOrElse("xmlRootTag", "root")

            //
            if ("text".equals(wholeFileFormat)) {

              //
              val wholeFileRDD = sparkSession
                .sparkContext
                .wholeTextFiles(wholeFilePath)

              //
              inStreamDf = WholeFileXMLReaderHelper.createWholeFileXMLDataFrame(wholeFileRDD, xmlTag)

            } else if ("sequence".equals(wholeFileFormat)) {

            }

          } else if ("hive".equals(step.format.getOrElse(""))) {
            //
            val sqlVal = stepAttrs.getOrElse("sql", "")
            val parSql = QueryParserHelper.replaceSessionArgs(sqlVal)
            println("before sql: " + sqlVal)
            println("after sql: " + parSql)
            inStreamDf = sparkSession.sql(parSql)

          } else if ("fixedwidth".equals(step.format.getOrElse(""))) {

            //
            // read fixedwidth files
            //
            val fixedRawDf = sparkSession.read
              .options(stepOpts)
              .text()
              .filter(r => !r.getAs[String](0).trim.isEmpty)

            // return
            inStreamDf = fixedRawDf

          } else if ("hbase".equals(step.format.getOrElse(""))) {

            //
            val hbaseDf = sparkSession
              .read
              .format("org.apache.spark.sql.execution.datasources.hbase")
              .option(HBaseTableCatalog.tableCatalog, SchemaBuilderHelper.createCatalog(stepAttrs))
              //.option("partitionColumn", "owner_id")
              //.option("lowerBound", 1)
              //.option("upperBound", 10000)
              .load()

            //hbaseDf.printSchema()
            //hbaseDf.show(false)

            // return
            inStreamDf = hbaseDf

          } else if ("jdbc".equals(step.format.getOrElse(""))) {

            //
            val jdbcDf = sparkSession
              .read
              .format(step.format.getOrElse(""))
              .options(stepOpts)
              //.option("partitionColumn", "owner_id")
              //.option("lowerBound", 1)
              //.option("upperBound", 10000)
              .load()

            //
            jdbcDf.printSchema()
            jdbcDf.show(false)

            // return
            inStreamDf = jdbcDf

          } else {

            //
            val fileFormatDf = sparkSession.read
              .options(stepOpts)
              .format(step.format.getOrElse("")) //"org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
            //.schema(SchemaBuilderHelper.createSchema(stepAttrs)) // create schema
            //.load()
            //.csv(stepAttrs.getOrElse("path", ""))

            //
            val fileSchemaDf =
              if (!stepAttrs.getOrElse("schemaCommand", "").isEmpty)
                fileFormatDf.schema(SchemaBuilderHelper.createSchema(stepAttrs))
              else
                fileFormatDf

            //
            val fileStreamDf = fileSchemaDf
              .load()

            //
            fileStreamDf.printSchema()
            fileStreamDf.show(2, false)

            // return
            inStreamDf = fileStreamDf
          }

          // save properties in session
          saveSessionData(step, inStreamDf)

          //
          // Run post process scripts
          if ("true".equals(stepAttrs.getOrElse("logStatus", "false"))) StatusBuilderHelper.beginLogStatus(step)

        }

      } // pre or step



      //
      inStreamDf
    }


  }

  /**
    * preferred factory method
    *
    * @param s
    * @return
    */
  def apply(s: String): ReaderBuilder = {
    getReaderBuilder(s)
  }

  // an alternative factory method (use one or the other)
  def getReaderBuilder(s: String): ReaderBuilder = {
    new BaseReader(s)
  }

}

/**
  * A Builder interface for all the Source Readers
  */
trait ReaderBuilder extends BaseTrait {

  var name: String = _

  /**
    * Returns DataFrame by reading data from source with defined type
    *
    * @param step
    * @param inDataFrame
    * @return
    */
  def readSource(step: Step, inDataFrame: Option[DataFrame] = None): DataFrame

}
