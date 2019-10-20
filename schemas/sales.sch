import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

def getSchema(): StructType = {
  val schema = StructType(
    Array(StructField("transactionId", StringType),
      StructField("customerId", StringType),
      StructField("itemId", StringType),
      StructField("amountPaid", StringType)))

  schema
}