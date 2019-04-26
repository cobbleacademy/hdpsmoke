package com.drake.plan

import com.drake.{BaseTrait, PropsUtil, SparkHelper}
import com.drake.model.Model.{FlowLevel, SplitDataFrame, Step}
import com.drake.reader.ReaderBuilder
import com.drake.writer.WriterBuilder
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.MutableList

/**
  * A Builder class for plan exeuctor
  */
object PlanExecutorBuilder {

  /**
    * Builds specific Plan Executor
    */
  class BasePlanExecutorBuilder(handlername: String) extends PlanExecutorBuilder {

    name = handlername


    private var flowLevels = MutableList[FlowLevel]()

    /**
      * Process Spouts to build Flow
      *
      * @param inDataFrames
      */
    private def createSpoutsLevel(inDataFrames: Map[String, DataFrame] = Map[String, DataFrame]()): Unit = {

      //
      // Handle Incoming Streams
      //
      PropsUtil.getWorkflow().steps.filter(x => !x.from.isDefined).map { ispout =>

        //
        val inputDataFrame: Option[DataFrame] = if (!inDataFrames.isEmpty) inDataFrames.get(ispout.name) else None

        println("Creating Stream......")

        // connect and build input stream as spark stream
        // for each spout id load corresponding stream
        val dataFrame = ReaderBuilder(ispout.name).readSource(ispout, inputDataFrame)

        // Named json stream
        var dataFrameList = List[DataFrame]()
        dataFrameList = dataFrameList ++ List(dataFrame)
        flowLevels += new FlowLevel(ispout.name, ispout.label.get, false, ispout.name, true, 1, 1, dataFrameList)

      }

    }


    /**
      * Find Targets based on the Source
      *
      * @param iSourceLabel
      * @return
      */
    private def getSourceBasedBolts(iSourceLabel: String): List[Step] = {
      // load all bolts belong to refSpout Name
      PropsUtil.getWorkflow().steps.filter(x => x.from.isDefined).filter(ibolt => {
        !ibolt.from.get.split(",").filter(grp => iSourceLabel.split(",").contains(grp)).isEmpty
      }).toList
    }


    /**
      * Create Target Levels for Sources at level
      *
      * @param sourceLevel
      * @return
      */
    private def createBoltsLevel(sourceLevel: Int = 1): String = {

      // process bolts for sourceLevel
      val iFlowLevels = flowLevels.filter(_.maxLevel.equals(sourceLevel))
      var contWithNextLevel = false

      //
      // Find Bolts connected to sources
      //
      iFlowLevels.map { iflowLevel =>

        var sourceBolts = getSourceBasedBolts(iflowLevel.label)

        // cache needed when targets are multiple
        if (sourceLevel > 2 && sourceBolts.size > 1) iflowLevel.cacheNeeded = true

        // each bolt under source
        sourceBolts.map { isourceBolt =>

          var iflowLevelList = flowLevels.filter { flowlevel => flowlevel.name.equals(isourceBolt.name) }
          iflowLevel.leafLevel = false

          // create level
          if (iflowLevelList.isEmpty) {
            var dataFrameList = List[DataFrame]()
            // define label or concatenated splitter labels
            if (!iflowLevel.dataFrames.isEmpty && iflowLevel.dataFrames.isDefinedAt(0) && sourceLevel == 1) dataFrameList = iflowLevel.dataFrames
            var splitLabelVar: String = isourceBolt.label.getOrElse("")
            if (isourceBolt.splitter.isDefined) {
              splitLabelVar = isourceBolt.splitter.map(f => f.splits.map(p => p.getOrElse("label", ""))).getOrElse(Seq("")).mkString(",")
            }
            val splitLabel = splitLabelVar
            //
            flowLevels += new FlowLevel(isourceBolt.name, splitLabel, false, (iflowLevel.displayName + " >> " + isourceBolt.name), true, sourceLevel + 1, sourceLevel + 1, dataFrameList)
            contWithNextLevel = true
          }
          else {
            // update level
            val iflowexist = iflowLevelList(0)
            iflowexist.maxLevel = sourceLevel + 1
            if (!iflowLevel.dataFrames.isEmpty && iflowLevel.dataFrames.isDefinedAt(0) && sourceLevel == 1) iflowexist.dataFrames = iflowexist.dataFrames ++ iflowLevel.dataFrames
            iflowexist.displayName = "(" + iflowexist.displayName + ") (" + iflowexist.displayName + " >> " + isourceBolt.name + ") "
            contWithNextLevel = true
          }

        }

      }

      // Now go ahead with next level
      if (contWithNextLevel) createBoltsLevel(sourceLevel + 1)

      "IGNORED"

    }



    /**
      * Create Level for Source and Targets
      *
      * @param inDataFrames
      */
    private def buildTopologyLevels(inDataFrames: Map[String, DataFrame] = Map[String, DataFrame]()): Unit = {

      //
      // Create Levels for spouts and bolts
      //
      createSpoutsLevel(inDataFrames)
      createBoltsLevel()

      // display names by level
      flowLevels.filter(_.leafLevel).map(x => println(x.displayName + "  " + x.minLevel))

    }



    /**
      * Process all Target DataFrames
      *
      * @param returnSink store or return dataframe
      * @param sourceLevel level to process from
      * @param outDataFrames return dataframes
      * @return
      */
    private def processTopologyTargets(
                                        returnSink: Boolean = false,
                                        sourceLevel: Int = 2,
                                        outDataFrames: Map[String, DataFrame] = Map[String, DataFrame]()): Map[String, DataFrame] = {
      logger.debug("processTopologyTargets invoked.....")

      //
      var resultDataFrames: Map[String, DataFrame] = outDataFrames


      // process bolts for sourceLevel
      val iFlowLevels = flowLevels.filter(_.maxLevel == sourceLevel)
      println("Found items size: " + iFlowLevels.size)
      var contWithNextLevel = false

      //
      // Find Bolts connected to sources
      //
      iFlowLevels.map { iflowLevel =>

        // process bolts
        val processBolts = PropsUtil.getWorkflow().steps.filter(x => x.from.isDefined).filter(_.name.equals(iflowLevel.name))

        // each bolt under source
        processBolts.map { iProcessBolt =>

          // union stream or single stream
          var unioineddataFrame = None: Option[DataFrame]
          var combinedNeeded = false

          iflowLevel.dataFrames.map { instream =>
            logger.info("adding source dataframe..." + instream.count())
            if (combinedNeeded) unioineddataFrame = Some(unioineddataFrame.get.union(instream)) else unioineddataFrame = Some(instream)
            combinedNeeded = true
          }

          // process the incoming stream
          println("Processing....", iProcessBolt.name)

          // editor vs sink process
          if (!iProcessBolt.model.equals("sink")) {
            // instantiate and delegate to Schema class
            val schemaKlass = Class.forName(iProcessBolt.command.get).getConstructor(iProcessBolt.name.getClass).newInstance(iProcessBolt.name).asInstanceOf[ {def buildEditor(step: Step, input: DataFrame): Seq[SplitDataFrame]}]

            // Default Schema Build
            val emitterSplitDataFrames = schemaKlass.buildEditor(iProcessBolt, unioineddataFrame.get)
            emitterSplitDataFrames.foreach(f => logger.info(" returned dataframe: " + f.label))

            // cache emitted stream when cache required
            if (iflowLevel.cacheNeeded) {
              logger.info("Cache needed for : " + iflowLevel.name)
              emitterSplitDataFrames.foreach(f => f.dataFrame.cache())
            }

            // 1 level below target bolts
            val targetChildBolts = getSourceBasedBolts(iflowLevel.label)

            // each bolt under this process bolt
            targetChildBolts.map { iChildBolt =>

              logger.info(" loading children for: " + iflowLevel.name + "(" + iflowLevel.label + ") and current child: " + iChildBolt.name + "(" + iChildBolt.from + ")" )

              val iCFlowLevels = flowLevels.filter(_.name.equals(iChildBolt.name))
              val fromLabels = iChildBolt.from.getOrElse("").split(",")

              //val emitterDataFrame = emitterSplitDataFrames.

              iCFlowLevels.foreach(icfLevel => {
                //
                emitterSplitDataFrames.foreach(s => {
                  if (fromLabels contains s.label) {
                    icfLevel.dataFrames = icfLevel.dataFrames :+ s.dataFrame
                  }
                })
                val emitterDataFrame = emitterSplitDataFrames.filter(p => p.label.equals(iChildBolt.from.getOrElse("")))(0).dataFrame
                icfLevel.dataFrames = icfLevel.dataFrames :+ emitterDataFrame
              })

              contWithNextLevel = true

            }

          } else {
            // store or return the Sink dataframe
            if (returnSink) {
              // Return the Sink Dataframe
              resultDataFrames += (iProcessBolt.name -> unioineddataFrame.get)
            } else {
              // Writer to store results without any return
              WriterBuilder(iProcessBolt.name).writeSink(iProcessBolt, unioineddataFrame.get)
            }
          }

        }

      }

      // Now go ahead with next level
      if (contWithNextLevel) processTopologyTargets(returnSink, (sourceLevel + 1), resultDataFrames)


      //
      resultDataFrames
    }




    /**
      * Executes the topology
      *
      */
    override def buildPlanExecutor(): Unit = {

      //
      // Handle Flow and create levels
      //
      buildTopologyLevels()

      //
      // Process targets based on level
      //
      processTopologyTargets()

      //
      // Wait for Termination of any query
      //
      if ("stream".equals(PropsUtil.getWorkflow().mode)) {
        SparkHelper.getSparkSession().streams.awaitAnyTermination()
      }

    }


    /**
      * Returns the transformed DataFrame
      *
      * @param inDataFrames
      * @return
      */
    override def buildPlanExecutor(inDataFrames: Map[String, DataFrame] = Map[String, DataFrame]()): Map[String, DataFrame] = {
      //
      var outDataFrames: Map[String, DataFrame] = Map[String, DataFrame]()

      //
      // Handle Flow and create levels
      //
      buildTopologyLevels(inDataFrames)

      //
      // Process targets based on level
      //
      outDataFrames = processTopologyTargets(true)

      //
      outDataFrames
    }

  }



  /**
    * preferred factory method
    *
    * @param s
    * @return
    */
  def apply(s: String = ""): PlanExecutorBuilder = {
    getPlanExecutorBuilder(s)
  }

  // an alternative factory method (use one or the other)
  def getPlanExecutorBuilder(s: String = ""): PlanExecutorBuilder = {
    new BasePlanExecutorBuilder(s)
  }

}

/**
  * A Builder interface for execution plan
  */
trait PlanExecutorBuilder extends BaseTrait {

  var name: String = _


  /**
    * Executes the topology
    *
    */
  def buildPlanExecutor(): Unit = {

    // TODO: Should be implemented by Child Classes

  }

  /**
    * Returns the transformed DataFrame
    *
    * @param inDataFrames
    * @return
    */
  def buildPlanExecutor(inDataFrames: Map[String, DataFrame] = Map[String, DataFrame]()): Map[String, DataFrame] = {
    //
    val outDataFrames: Map[String, DataFrame] = Map[String, DataFrame]()

    // TODO: Should be implemented by Child Classes

    //
    outDataFrames
  }

}
