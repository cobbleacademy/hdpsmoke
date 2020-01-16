import org.apache.spark.sql.types.{ArrayType,BinaryType,IntegerType,LongType,StringType,TimestampType,StructField,StructType}

def getSchema(): StructType = {
  val schema = new StructType()
    .add("catalog", new StructType()
        .add("book", new StructType()
            .add("id", StringType)
            .add("author", StringType)
            .add("title", StringType)
            .add("genre", StringType)
            .add("price", StringType)
            .add("publish_date", StringType)
            .add("description", StringType)
            )
            )

  schema
}