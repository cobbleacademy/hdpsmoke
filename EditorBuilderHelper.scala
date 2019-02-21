package com.drake.editor

import com.drake.function.Func
import com.drake.function.Func.{uuid, xmltojson}
import com.drake.reader.StaticSourceReader
import com.drake.{BaseTrait, PropsUtil, SessionDataHelper, SparkHelper}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable
import scala.xml._

/**
  * A common helper class for all functions needed in any EditorBuilder
  */
object EditorBuilderHelper extends BaseTrait {


  /**
    * Executes the trigger and saves the corresponding output in session map under triggerStep group
    *
    * @param triggerStep
    * @param commandPath
    * @param commandName
    */
  def executeTriggerCondition(triggerStep: String, commandPath: String, commandName: String, func: String = "", proc: String = ""): Unit = {

    //
    val sessDataMap = SessionDataHelper.getSessionData()
    val stepMap: mutable.Map[String, String] = mutable.Map("success" -> "false")
    sessDataMap += (triggerStep -> stepMap)

    // Run post process scripts
    import sys.process._
    val cmd = commandPath + "/" + commandName
    println("running trigger " + triggerStep + " and script: " + cmd)
    val pseq = Seq(cmd, func, proc)
    //val shellResult = Process(cmd+).lineStream //cmd.!!;val arr = shellResult.split("\\n")
    val shellResult = Process(pseq).lineStream //cmd.!!;val arr = shellResult.split("\\n")
    shellResult.foreach(x => {
      val arr = x.split("=")
      stepMap += (arr(0) -> arr(1))
    })
    println("Trigger status.." + triggerStep)
    stepMap.foreach(println)

  }


  /**
    * Builds the scheam from the given code
    *
    * @param schemaAttrs
    * @return
    */
  def createSchema(forName: String, schemaAttrs: Map[String, String]): StructType = {

    // instantiate and delegate to Schema class
    val klassName = schemaAttrs.getOrElse("schemaCommand", com.drake.schema.SchemaBuilder.DEFAULT_SCHEMA_BUILDER)
    val schemaKlass = Class.forName(klassName).getConstructor(forName.getClass).newInstance(forName).asInstanceOf[ {def buildSchema(schemaAttrs: Map[String, String]): StructType}]

    // Default Schema Build
    val readerSchema = schemaKlass.buildSchema(schemaAttrs)
    println(readerSchema)

    readerSchema
  }


  /**
    * Builds the FixedWidths from the given code
    *
    * @param schemaAttrs
    * @return
    */
  def createFixedWidths(forName: String, schemaAttrs: Map[String, String]): Seq[Int] = {

    // instantiate and delegate to Schema class
    val klassName = schemaAttrs.getOrElse("schemaCommand", com.drake.schema.SchemaBuilder.FIXED_WIDTH_SCHEMA_BUILDER)
    val schemaKlass = Class.forName(klassName).getConstructor(forName.getClass).newInstance(forName).asInstanceOf[ {def buildFixedWidths(schemaAttrs: Map[String, String]): Seq[Int]}]

    // FixedLength Build
    val readerFixedLengths = schemaKlass.buildFixedWidths(schemaAttrs)
    println(readerFixedLengths)

    readerFixedLengths
  }


  /**
    * Ruturns Function from the schema configuration file, this returned function is ready to be executed on data
    *
    * @param record
    * @param schemaAttrs
    * @return
    */
  def createContainsSchemaFunction(forName: String, schemaAttrs: Map[String, String]): (String => Boolean) = {

    // instantiate and delegate to Schema class
    val klassName = schemaAttrs.getOrElse("schemaCommand", com.drake.schema.SchemaBuilder.FIXED_WIDTH_SCHEMA_BUILDER)
    val schemaKlass = Class.forName(klassName).getConstructor(forName.getClass).newInstance(forName).asInstanceOf[ {def buildSchemaConformanceFunc(schemaAttrs: Map[String, String]): (String => Boolean)}]

    // FixedLength Build
    val readerFuncContainsSchema = schemaKlass.buildSchemaConformanceFunc(schemaAttrs)
    //println(readerFuncContainsSchema)

    //
    readerFuncContainsSchema
  }


  /**
    * Parses the input data into multiple strings split based on sequence of fixed widths
    *
    * @param line
    * @param lengths
    * @return
    */
  def parseFixedWidths(line: String, lengths: Seq[Int]): Seq[String] = {
    lengths.indices.foldLeft((line, Array.empty[String])) { case ((rem, fields), idx) =>
      val len = lengths(idx)
      val fld = rem.take(len).trim
      (rem.drop(len), fields :+ fld)
    }._2
  }


  /**
    * Returns DataFrame after processing category
    *
    * @param startDF
    * @param inclAttrs
    * @return
    */
  def includeAttributeTransform(inclAttrs: Map[String, String], startDF: DataFrame): DataFrame = {
    //
    //val sessDataMap = SessionDataHelper.getSessionData()
    val inclColsMap: mutable.Map[String, String] = mutable.Map[String, String]()

    //
    // include process
    //
    inclAttrs.map(x => {
      val key = x._1
      var value = x._2
      value = SessionDataHelper.parentageLookup(value)
      inclColsMap += (key -> value)
    })
    //
    val inclDf = inclColsMap.foldLeft(startDF)((df, c) =>
      df.withColumn(c._1, lit(c._2))
    )

    //
    inclDf.printSchema()
    inclDf
  }


  /**
    * Returns DataFrame after processing category
    *
    * @param startDF
    * @param startAttrs
    * @return
    */
  def categoryTransform(forName: String, occurrence: Int = 0, startAttrs: Map[String, String], startDF: DataFrame): DataFrame = {

    //
    val ss = startDF.sparkSession
    var categoryDfVar: DataFrame = startDF
    import ss.implicits._

    //
    Func.registerUUID(startDF)

    //
    val currCategory = startAttrs.getOrElse("category", "")
    val currCategoryKey = forName + "_" + currCategory + "_" + occurrence

    //
    if (!currCategory.isEmpty) {
      //
      var categoryDfCurrVar: DataFrame = startDF


      //
      //
      if ("sql".equals(startAttrs.getOrElse("category", ""))) {
        //
        if (startAttrs.get("currentTempView").isDefined) startDF.createOrReplaceTempView(startAttrs.getOrElse("currentTempView", ""))
        categoryDfCurrVar = ss.sql(startAttrs.getOrElse("sql", ""))
        //categoryDfCurrVar.printSchema()
        //categoryDfCurrVar.show(false)

        //
      } else if ("fromjsonschema".equals(startAttrs.getOrElse("category", ""))) {
        // create schema
        val readerSchema = createSchema(forName, startAttrs)

        //
        val jsonColumn = startAttrs.getOrElse("fromJsonColumn", "")
        val jsonAlias = startAttrs.getOrElse("fromJsonAlias", "")

        if ("" != jsonAlias) {
          categoryDfCurrVar = startDF
            .select(from_json(col(jsonColumn), readerSchema).alias(jsonAlias))
            .select(jsonAlias + ".*")
        }

      } else if ("toxmlschema".equals(startAttrs.getOrElse("category", ""))) {
        // create schema
        val readerSchema = createSchema(forName, startAttrs)

        //
        val valColumn = startAttrs.getOrElse("fromColumn", "")
        val valAlias = startAttrs.getOrElse("toAlias", "")

        if ("" != valAlias) {
          categoryDfCurrVar = startDF
            .select(from_json(xmltojson(col(valColumn)), readerSchema).alias(valAlias))
            .select(valAlias + ".*")
        }

        //
      } else if ("fixedwidthschema".equals(startAttrs.getOrElse("category", ""))) {
        // create schema
        StaticSourceReader.broadcastReference(currCategoryKey + "_schema", createSchema(forName, startAttrs))
        //val readerSchema = createSchema(forName, startAttrs)
        val readerSchema: StructType = StaticSourceReader.getBroadcastReferenceData(currCategoryKey + "_schema").asInstanceOf[StructType]
        StaticSourceReader.broadcastReference(currCategoryKey + "_lengths", createFixedWidths(forName, startAttrs))
        //val lengths = createFixedWidths(forName, startAttrs)
        val lengths: List[Int] = StaticSourceReader.getBroadcastReferenceData(currCategoryKey + "_lengths").asInstanceOf[List[Int]]


        // containsFunc takes string(input) and determines whether schema confirms or not
        ///
        ///
        ///
        ///
        val containsFunc = udf {
          new Function1[String, Boolean] with Serializable {

            import scala.reflect.runtime.currentMirror
            import scala.tools.reflect.ToolBox

            lazy val toolbox = currentMirror.mkToolBox()
            lazy val func = {
              println("reflected function") // triggered at every worker
              val schemaCode = PropsUtil.loadContainsSchemaCode(startAttrs.getOrElse("schemaPath", ""))
              toolbox.eval(toolbox.parse(schemaCode)).asInstanceOf[String => Boolean]
            }

            def apply(s: String): Boolean = func(s)
          }
        }
        ///
        ///
        ///
        ///
        val colNames = readerSchema.fieldNames
        val colTypes = readerSchema.fields.map(f => f.dataType)
        val fixedwidthsize = startAttrs.getOrElse("fixedwidthsize", "0").toInt
        logger.warn("Fixed schema requested Columns are " + colNames.size + " and lengths are " + lengths.size)


        /*
         * parse incoming string into array of fields
         */
        def parseFixedWidthsUdf = udf((line: String, lengthStr: String) => {
          val localLens = lengthStr.split(",")
          localLens.indices.foldLeft((line, Array.empty[String])) {
            case ((rem, fields), idx) => {
              val len = localLens(idx).toInt
              val fld = rem.take(len).trim
              (rem.drop(len), fields :+ fld)
            }
          }._2
        })

        //
        //val fwMapDf = startDF
        //  .filter(containsFunc(col("value")))
        //  .map(r => parseFixedWidths(r.getAs[String](0), lengths))
        //  .toDF()
        //
        val filteredFwMapDf = startDF
          .filter(containsFunc(col("value")))
        //
        val fwMapDf = filteredFwMapDf
          .withColumn("value", parseFixedWidthsUdf(col("value"), lit(lengths.mkString(","))))

        //
        val fwAllColsDf = lengths.indices.foldLeft(fwMapDf) { case (result, idx) =>
          result.withColumn(colNames(idx), $"value".getItem(idx).cast(colTypes(idx)))
        }
        //
        categoryDfCurrVar = fwAllColsDf.drop("value")
      }

      //categoryDfVar.show(false)
      categoryDfVar = categoryDfCurrVar
      val repartBy = startAttrs.getOrElse("repartitionBy", "")
      if (!repartBy.isEmpty) categoryDfVar = categoryDfCurrVar.repartition(repartBy.split(",").map(col(_)): _*)
    }

    //
    categoryDfVar.printSchema()
    categoryDfVar
  }



  /**
    * @param begin
    * @param iterDf
    * @param convArray
    * @return Returns DataFrame after processing categories in sequence of transformations
    */
  def recursiveCategoryTransform(forName: String, begin: Int, convArray: Array[Map[String, String]], iterDf: DataFrame): DataFrame = {

    //
    var iterCurrDfVar: DataFrame = iterDf
    val ss: SparkSession = iterDf.sparkSession

    //
    val iterConvAttrGroup = convArray.filter(x => begin == x.getOrElse("seq", "-1").toInt)
    //
    if (!iterConvAttrGroup.isEmpty) {
      //
      val iterConvAttrCurr: Map[String, String] = iterConvAttrGroup(0)
      val nextIterCurrDf: DataFrame = categoryTransform(forName, begin, iterConvAttrCurr, iterDf)

      //
      iterCurrDfVar = recursiveCategoryTransform(forName, (begin + 1), convArray, nextIterCurrDf)
    }

    //
    iterCurrDfVar
  }


  /**
    * Adds Stats column for each type of invalid configuration
    *
    * @param stepAttrs
    * @param input
    * @return
    */
  def addColumnValidationStats(stepAttrs: Map[String, String], input: DataFrame): DataFrame = {

    //
    var addColStatDfVar: DataFrame = input

    // load invalid config for given src_system and table
    val configArr = StaticSourceReader.getInvCfgData()

    // RUN ALL INVALID FUNCTIONS IN SEQUENCE

    //
    def rowDataTypeChk = udf((row: Row) => {

      val chkCols = configArr.filter(p => p.getAs[Int]("data_type_check") == 1)
      var colList = ""

      chkCols.map(chkCol => {
        val colName = chkCol.getAs[String]("col_name")
        val dataTypeName = chkCol.getAs[String]("data_type")
        try {
          //row.getAs(colName).getClass
          dataTypeName match {
            case "Int" => if (row.getAs(colName).toString.equals(row.getAs[Int](colName).toString.toInt)) row.getAs[Double](colName).getClass
            case "Long" => if (row.getAs(colName).toString.equals(row.getAs[Long](colName).toString.toLong)) row.getAs[Double](colName).getClass //if (!row.getAs(colName).isInstanceOf[Long]) row.getAs(colName).getClass //println("Verifying Long :" + row.getAs(colName).getClass+ " " + row.getAs(colName).isInstanceOf[Long] + " "+colName)
            case "String" => if (row.getAs(colName).toString.equals(row.getAs[String](colName).toString)) row.getAs[Double](colName).getClass
            case "Float" => if (row.getAs(colName).toString.equals(row.getAs[Float](colName).toString.toFloat)) row.getAs[Double](colName).getClass
            case "Double" => if (row.getAs(colName).toString.equals(row.getAs[Double](colName).toString.toDouble)) row.getAs[Double](colName).getClass
            case _ => ""
          }
        } catch {
          case x: Throwable => if (colList.length > 0) colList += "~" + colName else colList += colName
        }
      })
      colList
    })

    //
    def rowNullChk = udf((row: Row) => {

      val chkCols = configArr.filter(p => p.getAs[Int]("null_check") == 1)
      var colList = ""

      chkCols.map(chkCol => {
        val colName = chkCol.getAs[String]("col_name")
        if (row.getAs[String](colName) == null) {
          if (colList.length > 0) colList += "~" + colName else colList += colName
        }
      })
      colList
    })

    //
    def rowEmptyChk = udf((row: Row) => {

      val chkCols = configArr.filter(p => p.getAs[Int]("empty_check") == 1)
      var colList = ""

      chkCols.map(chkCol => {
        val colName = chkCol.getAs[String]("col_name")
        if (row.getAs[String](colName) == "") {
          if (colList.length > 0) colList += "~" + colName else colList += colName
        }
      })
      colList
    })


    //
    addColStatDfVar = input
      .withColumn("datatypechkstat", rowDataTypeChk(struct(input.columns.map(col): _*)))
      .withColumn("nullchkstat", rowNullChk(struct(input.columns.map(col): _*)))
      .withColumn("emptychkstat", rowEmptyChk(struct(input.columns.map(col): _*)))


    //
    addColStatDfVar
  }


  /**
    * Saved the summation of each column stat on validation
    *
    * @param stepAttrs
    * @param statDf
    * @param input
    */
  def saveColStatValidationSummations(stepAttrs: Map[String, String], statDf: DataFrame, input: DataFrame): Unit = {

    // RUN SUMMATATIONs on ABOVE FUNCTIONS

    //
    val configArr = StaticSourceReader.getInvCfgData()

    //
    val totalRecCount = input.count
    var mutColChkMap = mutable.Map[String, mutable.Map[String, Long]]()
    val colDupChkStat = "sum_dup_chk_cnt"
    configArr.foreach(chkCol => {
      val colName = chkCol.getAs[String]("col_name")
      val cnt = input.groupBy(colName).count.filter("count = 1").count
      var innrMap = mutColChkMap.getOrElse(colDupChkStat, mutable.Map[String, Long]())
      innrMap += colName -> (totalRecCount - cnt)
      mutColChkMap += colDupChkStat -> innrMap
    })



    //
    List("sum_range_chk_cnt", "sum_const_chk_cnt")
      .foreach(sumCol => {
        configArr.foreach(chkCol => {
          val colName = chkCol.getAs[String]("col_name").toString
          var innrMap = mutColChkMap.getOrElse(sumCol, mutable.Map[String, Long]())
          innrMap += colName -> 0
          mutColChkMap += sumCol -> innrMap
        })
      })
    //
    val sumColMap = Map(
      "sum_dtype_chk_cnt" -> "datatypechkstat",
      "sum_null_chk_cnt" -> "nullchkstat",
      "sum_empty_chk_cnt" -> "emptychkstat"
    )
    sumColMap.foreach(sumCol => {
      configArr.foreach(chkCol => {
        val colName = chkCol.getAs[String]("col_name").toString
        val cnt = statDf.filter(col(sumCol._2).contains(colName)).count
        var innrMap = mutColChkMap.getOrElse(sumCol._1, mutable.Map[String, Long]())
        innrMap += colName -> cnt
        mutColChkMap += sumCol._1 -> innrMap
      })
    })


    //
    StaticSourceReader.broadcastSumChkColReference(mutColChkMap.map(f => f._1 -> f._2.toMap).toMap)
    var immutColChkMap = StaticSourceReader.getSumChkColData()

    //
    val sumChkStatRawDF = StaticSourceReader.getSampleSumChkColDataFrame(stepAttrs)
    val sumChkStatInitDF = immutColChkMap.keys.foldLeft(sumChkStatRawDF)((sumChkStatRawDF, colNm) => sumChkStatRawDF.withColumn(colNm, lit("0")))

    //
    //
    def rowReplaceChkVal = udf((colNm: String, colGrp: String) => {
      val ret: Long = immutColChkMap.getOrElse(colGrp, mutable.Map[String, Long]()).getOrElse(colNm, 0)
      ret
    })

    //
    val sumChkStatDF = immutColChkMap.keys.foldLeft(sumChkStatInitDF)((sumChkStatInitDF, colNm) => sumChkStatInitDF.withColumn(colNm, rowReplaceChkVal(col("col_name"), lit(colNm))))


    // additional columns event/process timestamps
    val sumChkHistMap = Map(
      "src_system" -> "$args#0",
      "event_dt" -> "$args#1",
      "event_hr" -> "$args#2",
      "process_epoch" -> "$sys#processEpochSec",
      "process_dt" -> "$sys#processDt",
      "process_hr" -> "$sys#processHr"
    )
    // Final sumchkstat with additional columns
    val sumChkStatFnlDF = EditorBuilderHelper.includeAttributeTransform(sumChkHistMap, sumChkStatDF) //includeAttributeTransform(sumChkStatDF, sumChkHistMap)
    sumChkStatFnlDF.createOrReplaceTempView("sumChkStatFnlDF")
    val qualStatTbl = PropsUtil.getWorkflow().qualityStatus.getOrElse("")
    val iostatqry =
      s"""
         |insert into table $qualStatTbl partition(src_system,event_dt)
         |select table_name,col_name,sum_dtype_chk_cnt,sum_null_chk_cnt,
         |sum_empty_chk_cnt,sum_dup_chk_cnt,sum_range_chk_cnt,sum_const_chk_cnt,
         |event_hr,process_epoch,process_dt,process_hr,src_system,event_dt
         | from sumChkStatFnlDF
      """.stripMargin
    SparkHelper.getSparkSession().sql(iostatqry)

    //sumChkStatFnlDF.show(false)
    //sess.sql("select * from invalid_col_stats").show(false)

  }


  /**
    * Adds Stat column for every event in the dataframe by comibining invalid status for all the columns in a row
    *
    * @param statDf
    * @return
    */
  def addInvalidColStat(statDf: DataFrame): DataFrame = {

    //
    var invColStDfVar: DataFrame = statDf


    //
    def concatColChkPrefix = udf((pfx: String, colVal: String) => {
      if (!colVal.isEmpty) pfx + colVal else ""
    })


    // create stat column to capture combined invalid reason
    val preStatChkStatDF = statDf
      .withColumn("uuid", uuid())
      .withColumn("stat", concat_ws(" ", concatColChkPrefix(lit("DataTypeCheck:"), col("datatypechkstat")), concatColChkPrefix(lit("NullCheck:"), col("nullchkstat")), concatColChkPrefix(lit("EmptyCheck:"), col("emptychkstat"))))
    invColStDfVar = preStatChkStatDF.withColumn("stat", trim(col("stat")))


    //
    invColStDfVar
  }


  /**
    * Returns Dataframe by including stat column from all invalid checks
    *
    * @param stepAttrs
    * @param input
    * @return
    */
  def invalidConfigTransform(stepAttrs: Map[String, String], input: DataFrame): DataFrame = {

    //
    // Add Column Stat on validation
    //
    var colStatDfVar: DataFrame = input
    colStatDfVar = EditorBuilderHelper.addColumnValidationStats(stepAttrs, input)
    val colStatDf = colStatDfVar



    //
    // Add Summ of Column Stat on validation
    //
    EditorBuilderHelper.saveColStatValidationSummations(stepAttrs, colStatDf, input)


    //
    // Convert individual column level stats into single stat column
    //
    var statDfVar: DataFrame = colStatDf
    statDfVar = EditorBuilderHelper.addInvalidColStat(colStatDf)
    val statDf = statDfVar


    //
    statDf
  }


  /**
    * Builds specific EditorBuilderHelper
    */
  class BaseEditorBuilderHelper(handlername: String, forhandlername: String) extends EditorBuilderHelper {

    //
    name = handlername
    forName = forhandlername

  }

  /**
    * preferred factory method
    *
    * @param s
    * @return
    */
  def apply(s: String, fs: String): EditorBuilderHelper = {
    new BaseEditorBuilderHelper(s, fs)
  }

  // an alternative factory method (use one or the other)
  def getEditorBuilderHelper(s: String, fs: String): EditorBuilderHelper = {
    new BaseEditorBuilderHelper(s, fs)
  }

}

/**
  * A common helper class for all functions needed in any EditorBuilder
  */
trait EditorBuilderHelper extends BaseTrait {

  var name: String = _
  var forName: String = _

}
