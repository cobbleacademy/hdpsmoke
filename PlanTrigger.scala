package com.drake

import com.drake.PlanExecutor.{buildTopologyLevels, processTopologyTargets}
import com.drake.editor.EditorBuilderHelper

/**
  * A trigger handler to run the Pipeline based on trigger scripts output
  */
object PlanTrigger extends BaseTrait {



  def triggerPlanExecutor(): Unit = {
    val TRIG_PRE = "preTrigger"
    val TRIG_ONFAILURE = "onFailTrigger"
    val TRIG_POST = "postTrigger"
    val SUCCESS_KEY = "success"

    //
    // Check pre condition trigger results
    //
    val workflow = PropsUtil.getWorkflow()
    EditorBuilderHelper.executeTriggerCondition(TRIG_PRE, workflow.triggerScriptPath.get, workflow.preTrigger.get)
    val trigMap: Map[String, String] = SessionDataHelper.getSessionData().getOrElse(TRIG_PRE, Map()).toMap

    //
    if ("true".equals(trigMap.getOrElse(SUCCESS_KEY, "false"))) {

      //
      try
      {
        //
        PlanExecutor.executeTopology()

        //
        EditorBuilderHelper.executeTriggerCondition(TRIG_POST, workflow.triggerScriptPath.get, workflow.postTrigger.get)

      } catch {
        case e: Exception => {
          EditorBuilderHelper.executeTriggerCondition(TRIG_ONFAILURE, workflow.triggerScriptPath.get, workflow.onFailTrigger.get)
          e.printStackTrace
        }
      }

    } else {

      println("***************************************************************************************")
      println("                            MBR AVAIL TRIGGER IS MISSING                               ")
      println("***************************************************************************************")

    }

  }


}
