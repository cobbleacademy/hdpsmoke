package com.drake

import com.drake.plan.PlanExecutorBuilder


/**
  * A pipeline runs in batch mode which reads from sources and persists the output
  */
object BatchPipeline extends BaseTrait {

  /**
    * An entry point to run pipeline of events
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    //
    PropsUtil.initialize()

    //
    SparkHelper.buildSparkSession()

    //
    SessionDataHelper.buildSessionData(args)

    //
    PlanExecutorBuilder().buildPlanExecutor()

  }


}
