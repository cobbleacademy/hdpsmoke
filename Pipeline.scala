package com.drake

import com.drake.model.Model.Audit
import com.drake.offset.OffsetStore
import com.drake.reader.AuditWriter
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

/**
  * A Pipeline runs actions in the dependency order to read, transform and persist the data
  */
object Pipeline {

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
    //SparkHelper.getSparkSession().streams.addListener(new DsStreamListener())

    //
    PlanExecutor.executeTopology()

  }

  class DsStreamListener extends StreamingQueryListener {
    override def onQueryStarted(event: QueryStartedEvent): Unit = { println("Query Started: " + event.name) }
    override def onQueryProgress(event: QueryProgressEvent): Unit = {
      println("Query in progress")
      //StaticSourceHelper.refreshStaticSources()
      //println(event.progress)
      println("*********************Custom Progress******************")
      println("Sink Path: " + event.progress.sink.description)
      val sOffsets = event.progress.sources.map(_.startOffset).mkString
      val eOffsets = event.progress.sources.map(_.endOffset).mkString
      println("Sink Num of InputRows: " + event.progress.numInputRows)
      println("Sink Input Rows Per Second: " + event.progress.inputRowsPerSecond)
      println("Sink Processed Rows Per Second: " + event.progress.processedRowsPerSecond)
      if (event.progress.numInputRows != 0) {
        val audit = Audit(event.progress.id.toString, event.progress.runId.toString, event.progress.sink.description, event.progress.numInputRows, event.progress.inputRowsPerSecond, event.progress.processedRowsPerSecond, sOffsets, eOffsets)
        AuditWriter("Audit").storeAuditRef(audit)
      }
      event.progress.sources.foreach(x => {
        OffsetStore.getOffsetStore.saveOffsets((if(x.startOffset eq null) x.endOffset else x.startOffset), PropsUtil.propValue("kafka.group-id"))
      })
      println("*********************Custom Progress******************")
    }
    override def onQueryTerminated(event: QueryTerminatedEvent): Unit = { println("Query Terminated: " + event.runId) }
  }

}

