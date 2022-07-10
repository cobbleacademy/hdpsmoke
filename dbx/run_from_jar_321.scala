// Databricks notebook source
// MAGIC %md #Load Date Dimension

// COMMAND ----------

dbutils.widgets.text("inputTableName","date_dim","Table Name")

// COMMAND ----------

import com.databricks.blog.datamart212.util.DateDimension

// COMMAND ----------

val dateDim = new DateDimension().createDataFrame()

// COMMAND ----------

spark.sql("drop table if exists "+ dbutils.widgets.get("inputTableName"))
dateDim.write.mode("overwrite").saveAsTable(dbutils.widgets.get("inputTableName"))

// COMMAND ----------

// MAGIC %md #Validate Date Dimension

// COMMAND ----------

// MAGIC %sql select * from date_dim

// COMMAND ----------

// MAGIC %md ##Check For Duplicates
// MAGIC * Should return zero

// COMMAND ----------

// MAGIC %sql
// MAGIC select date_key, count(*) 
// MAGIC from date_dim
// MAGIC group by date_key
// MAGIC having count(*) > 1  

// COMMAND ----------

// MAGIC %md ##Check for Leap Years

// COMMAND ----------

// MAGIC %sql
// MAGIC select year, count(*) as days_in_year
// MAGIC from date_dim
// MAGIC group by year
// MAGIC order by year  

// COMMAND ----------

// MAGIC %sql
// MAGIC describe extended date_dim
