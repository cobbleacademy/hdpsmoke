package com.delta.structs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._



object SSClkImpRate {


  val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark session")
      .config("spark.sql.shuffle.partitions", "1")
//      .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
//      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
//      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
  }

  spark.conf.set("spark.sql.shuffle.partitions", "1")

  import spark.implicits._

  def main1(args: Array[String]): Unit = {
    val impressions = spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", "5")
      .option("numPartitions", "1")
      .load()
      .select($"value".as("adId"), $"timestamp".as("impressionTime"))

    //impressions.show()

    var final_df = impressions.select("adId").groupBy("adId").count()
    final_df.printSchema()
    final_df.writeStream.outputMode("complete").option("truncate", false)
      .format("console").start().awaitTermination()

    impressions.writeStream.format("console")
      .outputMode("complete").start()             // Start the computation
      .awaitTermination()
  }


  def main2(args: Array[String]): Unit = {
    val impressions = spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", "5")
      .option("numPartitions", "1")
      .load()
      .select($"value".as("adId"), $"timestamp".as("impressionTime"))
      //.select(concat($"adId", lit(","), $"impressionTime"))
      .select(to_json(struct($"adId", $"impressionTime")).alias("content"))

    impressions.printSchema()

    val query = impressions
      .writeStream
      .outputMode("append")
//      .format("console")
      .format("text")
      .option("path", "output/multiple/")
      .option("checkpointLocation", "checkpoint/multiple")
      .start()

    query.awaitTermination()
  }


  def main3(args: Array[String]): Unit = {
    //    val schema = StructType(
    //      Array(StructField("customer_id", StringType),
    //        StructField("pid", StringType),
    //        StructField("pname", StringType),
    //        StructField("date", StringType)))

    //stream the orders from the csv files.
    val catgDF = spark
      .readStream
      .format("text")
      .option("path", "input/multiple")
      .load()


    val query = catgDF
      .writeStream
      //.queryName("category_books")
      //.format("console")
      .outputMode("append")
      .format("text")
      //.partitionBy("date")
      .option("path", "output/multiple/")
      .option("checkpointLocation", "checkpoint/multiple")
      .start()

    query.awaitTermination()
  }

  def ratetokafka1(args: Array[String]): Unit = {
    val impressions = spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", "2")
      .option("numPartitions", "1")
      .load()
      .select($"value".as("adId"), $"timestamp".as("impressionTime"))
      //.select(concat($"adId", lit(","), $"impressionTime"))
      .select($"adId".as("key"), to_json(struct($"adId", $"impressionTime")).alias("value"))
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    impressions.printSchema()

    val query = impressions

      .writeStream
      .format("kafka")
      .outputMode("append")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "inrate")
      .option("checkpointLocation", "checkpoint/ratetokafka")
      .start()

    query.awaitTermination()
  }


  def kafkatokafka1(args: Array[String]): Unit = {
    val impressions = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "inrate")
      .option("startingOffsets", "earliest")
      //.option("startingOffsets", """{"inrate":{"0":-2}}""")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    impressions.printSchema()

    val query = impressions
      .writeStream
      .format("kafka")
      .outputMode("append")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "outrate")
      .option("checkpointLocation", "checkpoint/kafkatokafka")
      .start()

    query.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    kafkatokafka1(args)
  }

}
