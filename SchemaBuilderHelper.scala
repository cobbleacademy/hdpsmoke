package com.drake.schema

import org.apache.spark.sql.types.{StringType, StructType}

/**
  * A Helper class to create schema, catalog, fixed width structures for reader and writer
  */
object SchemaBuilderHelper {


  /**
    * Returns schema of single value with StringType
    * @return
    */
  def createWholeFileTextSchema(): StructType = {
    return new StructType().add("value", StringType)
  }

  /**
    * Builds the schema from the given code from file
    *
    * @param schemaAttrs
    * @return
    */
  def createSchema(schemaAttrs: Map[String, String]): StructType = {

    // instantiate and delegate to Schema class
    val name = com.drake.schema.SchemaBuilder.DEFAULT_SCHEMA_BUILDER.getClass.getName
    val klassName = schemaAttrs.getOrElse("schemaCommand", com.drake.schema.SchemaBuilder.DEFAULT_SCHEMA_BUILDER)
    val schemaKlass = Class.forName(klassName).getConstructor(name.getClass).newInstance(name).asInstanceOf[ {def buildSchema(schemaAttrs: Map[String, String]): StructType}]

    // Default Schema Build
    val schema = schemaKlass.buildSchema(schemaAttrs)
    //println(schema)

    //
    schema
  }



  /**
    * Builds the catalog from the given code from file
    *
    * @param schemaAttrs
    * @return
    */
  def createCatalog(schemaAttrs: Map[String, String]): String = {

    // instantiate and delegate to Schema class
    val name = com.drake.schema.SchemaBuilder.DEFAULT_SCHEMA_BUILDER.getClass.getName
    val klassName = schemaAttrs.getOrElse("schemaCommand", com.drake.schema.SchemaBuilder.DEFAULT_SCHEMA_BUILDER)
    val schemaKlass = Class.forName(klassName).getConstructor(name.getClass).newInstance(name).asInstanceOf[ {def buildCatalog(schemaAttrs: Map[String, String]): String}]

    // Default Schema Build
    val schema = schemaKlass.buildCatalog(schemaAttrs)
    //println(schema)

    //
    schema
  }


}
