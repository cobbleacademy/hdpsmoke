package com.drake.model

import org.apache.spark.sql.DataFrame

/**
  * A Model to define common case classes
  */
object Model {

  case class OffsetSuite(suite: Map[String, Map[String, Long]])

  case class Attribute(key: String, value: String)

  case class SplitDataFrame(label: String, dataFrame: DataFrame)

  case class Splitter(tempView: String, splits: Seq[Map[String, String]])

  case class Step(name: String, model: String, from: Option[String], format: Option[String], command: Option[String], label: Option[String], options: Option[Map[String, String]], attributes: Option[Map[String, String]], conversions: Option[Array[Map[String, String]]], include: Option[Map[String, String]], post: Option[Map[String, String]], splitter: Option[Splitter])

  case class Workflow(process: String, mode: String, qualityConfig: Option[String], qualityStatus: Option[String], triggerScriptPath: Option[String], preTrigger: Option[String], onFailTrigger: Option[String], postTrigger: Option[String], steps: Seq[Step], attributes: Option[Seq[Attribute]])

  case class Record()

  case class FlowLevel(name: String,
                       label: String,
                       var cacheNeeded: Boolean = false,
                       var displayName: String,
                       var leafLevel: Boolean = true,
                       minLevel: Int = 1,
                       var maxLevel: Int = 1,
                       var dataFrames: List[DataFrame]
                      )


  case class Audit(id: String, runId: String, path: String, numInputRows: Long, inputRowsRate: Double, processedRowsRate: Double, startOffset: String, endOffset: String)
}
