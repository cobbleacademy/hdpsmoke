package com.drake

import com.drake.plan.PlanExecutorBuilder
import org.apache.spark.sql.DataFrame


/**
  * A pipeline runs in real-time mode which reads dataset and returns another dataset
  */
object RTPipeline extends BaseTrait {

  /**
    * An entry point to run pipeline of events
    *
    * @param args
    */
  def executePipeline(inDataFrames: Map[String, DataFrame]): Map[String, DataFrame] = {

    //
    var outDataFrames: Map[String, DataFrame] = Map[String, DataFrame]()

    //
    PropsUtil.initialize()

    //
    SparkHelper.buildSparkSession()

    //
    SessionDataHelper.buildSessionData(Array[String]())

    //
    outDataFrames = PlanExecutorBuilder().buildPlanExecutor()


    //
    outDataFrames
  }


}
