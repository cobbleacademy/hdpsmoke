package com.delta.lake

import com.delta.SparkSessionWrapper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.delta.DeltaLog


object SparkLake extends SparkSessionWrapper {

  import spark.implicits._

  def createDeltaLake(): Unit = {

    val path = new java.io.File("./src/test/resources/person/").getCanonicalPath
    val df = spark
      .read
      .option("header", "true")
      .option("charset", "UTF8")
      .csv(path)

    val outputPath = new java.io.File("./tmp/delta_lake/").getCanonicalPath
    df
      .repartition(5)
      .write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .save(outputPath)

  }

  def displayDeltaLake(): Unit = {

    val path = new java.io.File("./tmp/delta_lake/").getCanonicalPath

    val df = spark
      .read
      .format("delta")
      .option("versionAsOf", 2)
      .load(path)

    df.show()

  }

  def compactDeltaLake(): Unit = {

    val path = new java.io.File("./tmp/delta_lake/").getCanonicalPath

    val df = spark
      .read
      .format("delta")
      .load(path)

    df
      .repartition(1)
      .write
      .format("delta")
      .mode("overwrite")
      .save(path)

  }

  def vacuumDeltaLake(): Unit = {

    val path = new java.io.File("./tmp/delta_lake/").getCanonicalPath
    import io.delta.tables._
    val deltaTable = DeltaTable.forPath(spark, path)
    deltaTable.vacuum(0.000001)

  }

  def displayDeltaLog(): Unit = {

    val path = new java.io.File("./tmp/delta_lake/").getCanonicalPath
    DeltaLog
      .forTable(spark, path)
      .snapshot
      .allFiles
      .show(false)

    DeltaLog
      .forTable(spark, path)
      .snapshot
      .allFiles
      .printSchema()

  }

  def showNum1GbPartitions(): Unit = {

    val path = new java.io.File("./tmp/delta_lake/").getCanonicalPath
    val numBytes = DeltaLog
      .forTable(spark, path)
      .snapshot
      .allFiles
      .agg(sum("size"))
      .head
      .getLong(0)
    val numGigabytes = numBytes / 1073741824L
    val num1GBPartitions = if (numGigabytes == 0L) 1 else numGigabytes.toInt

  }

  def createPartitionedDeltaLake1(): Unit = {

    val path = new java.io.File("./tmp/delta_lake/").getCanonicalPath
    val df = spark
      .read
      .format("delta")
      .load(path)

    val outputPath = new java.io.File("./tmp/partitioned1/").getCanonicalPath
    df
      .repartition(col("country"))
      .write
      .format("delta")
      .partitionBy("country")
      .save(outputPath)

  }

  def filterPartitionedDeltaLake1(): Unit = {

    val path = new java.io.File("./tmp/partitioned1/").getCanonicalPath
    val df = spark
      .read
      .format("delta")
      .load(path)

    df
      .where($"country" === "Russia" && $"first_name".startsWith("M"))
      .explain()

  }

  def createPartitionedDeltaLake2(): Unit = {

    val path = new java.io.File("./tmp/delta_lake/").getCanonicalPath
    val df = spark
      .read
      .format("delta")
      .load(path)
      .repartition(5)

    // writes out tons of files

    val outputPath = new java.io.File("./tmp/partitioned2/").getCanonicalPath
    df
      .write
      .format("delta")
      .partitionBy("country")
      .save(outputPath)

  }

  def createPartitionedDeltaLake3(): Unit = {

    val path = new java.io.File("./tmp/delta_lake/").getCanonicalPath
    val df = spark
      .read
      .format("delta")
      .load(path)
      .repartition(5)

    // max 100 files per partition
    val outputPath = new java.io.File("./tmp/partitioned3/").getCanonicalPath
    df
      .repartition(100, $"country", rand)
      .write
      .format("delta")
      .partitionBy("country")
      .save(outputPath)

  }

  def displayPartitionedDeltaLog(): Unit = {

    val path = new java.io.File("./tmp/partitioned3/").getCanonicalPath
    DeltaLog
      .forTable(spark, path)
      .snapshot
      .allFiles
      .show(false)

    DeltaLog
      .forTable(spark, path)
      .snapshot
      .allFiles
      .printSchema()

  }

  def createDeltaLakeWithRussia(): Unit = {

    val path = new java.io.File("./src/test/resources/person/").getCanonicalPath
    val df = spark
      .read
      .option("header", "true")
      .option("charset", "UTF8")
      .csv(path)

    val outputPath = new java.io.File("./tmp/delete_example/").getCanonicalPath
    df
      .repartition(1)
      .write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .save(outputPath)

  }

  def filterRussianData(): DataFrame = {

    val path = new java.io.File("./src/test/resources/person/").getCanonicalPath
    val df = spark
      .read
      .option("header", "true")
      .option("charset", "UTF8")
      .csv(path)
    df.where(col("country") =!= "Russia")

  }

  def deleteRussianData(): Unit = {

    import io.delta.tables._

    val path = new java.io.File("./tmp/delete_example/").getCanonicalPath
    val deltaTable = DeltaTable.forPath(spark, path)
    deltaTable.delete(condition = expr("country == 'Russia'"))

    deltaTable.toDF.show()

  }

  def appendContinent(): DataFrame = {

    def withContinent()(df: DataFrame): DataFrame = {
      df.withColumn(
        "continent",
        when(col("country") === "Russia", "Europe")
          .when(col("country") === "China", "Asia")
          .when(col("country") === "Argentina", "South America")
      )
    }

    val path = new java.io.File("./src/test/resources/person/").getCanonicalPath
    val df = spark
      .read
      .option("header", "true")
      .option("charset", "UTF8")
      .csv(path)
    df.transform(withContinent())

  }

  def appendContinentInDelta(): Unit = {

    def withContinent()(df: DataFrame): DataFrame = {
      df.withColumn(
        "continent",
        when(col("country") === "Russia", "Europe")
          .when(col("country") === "China", "Asia")
          .when(col("country") === "Argentina", "South America")
      )
    }

    val path = new java.io.File("./tmp/delta_lake/").getCanonicalPath

    val df = spark
      .read
      .format("delta")
      .load(path)

    df
      .transform(withContinent())
      .write
      .format("delta")
      .option("mergeSchema", "true")
      .mode(SaveMode.Append)
      .save(path)

  }

}