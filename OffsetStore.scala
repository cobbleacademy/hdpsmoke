package com.drake.offset

import java.util.Date
import java.util.concurrent.CountDownLatch
import java.util.ArrayList

import com.drake.BaseTrait
import com.drake.model.Model.OffsetSuite
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.apache.zookeeper.ZooDefs.{Perms,Ids}
import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooKeeper}
import org.apache.zookeeper.data.{ACL,Stat}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import kafka.utils.ZKGroupTopicDirs
import kafka.utils.ZkUtils
import org.json4s.DefaultFormats
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read, write}

import scala.collection.JavaConverters
import scala.collection.mutable.{LazyBuilder, ListBuffer}
import scala.util.matching.Regex

/**
  * An offset store for streaming pub-sub queues, all offsets should be stored in permanent storage to restart the pipeline
  * where it was left off before it got killed
  */
trait OffsetStore extends BaseTrait {

  /**
    * Read offsets when exists otherwise return configured offset for startingOffsets in properties
    * @param topics
    * @param groupId
    * @param defaultValue
    * @return
    */
  def readOffsets(topics: Seq[String], groupId: String, defaultValue: String): String


  /**
    * Save offsets to zookeeper path
    * @param offsets
    * @param groupId
    */
  def saveOffsets(offsets: String, groupId: String): Unit


}

/**
  * An object offset store to initialize corresponding persisten store for offsets
  */
object OffsetStore extends BaseTrait {

  var storeBuilder: Builder = _
  var offsetStore: OffsetStore = _

  import scala.collection.JavaConverters._

  /**
    * Zookeeper offset store for offsets
    */
  class ZookeeperOffsetStore extends OffsetStore {

    private var zk: ZooKeeper = _
    private var lPath: String = _
    private var lHost: String = _
    val connSignal = new CountDownLatch(1)
    private var zkUtils: ZkUtils = _

    //
    case class ZKWatcher() extends Watcher {
      def process(event: WatchedEvent) = {
        if(event.getState() == KeeperState.SyncConnected)
          connSignal.countDown()
      }
    }


    /**
      * Initialize
      * @param host
      * @param basePath
      * @return
      */
    def initialize(host: String): ZkUtils = {
      val zkClientAndConnection = ZkUtils.createZkClientAndConnection(lHost, 10000, 60000)
      val lzkUtils = new ZkUtils(zkClientAndConnection._1, zkClientAndConnection._2, false)
      zkUtils = lzkUtils
      zkUtils
    }


    /**
      * Initialize
      * @param host
      * @param basePath
      * @return
      */
    def initialize(host: String, basePath: String): ZooKeeper = {
      lPath = basePath
      val wtc = new ZKWatcher()
      val lzk = new ZooKeeper(host, 5000, wtc)
      connSignal.await()
      zk = lzk
      saveNode(basePath, new Date().toString.getBytes)
      lzk
    }


    /**
      * Check if node exists
      * @param path
      * @return
      */
    def existNode(path: String): Stat = {
      zk.exists(path, true)
    }


    /**
      * Read Node
      * @param path
      * @return
      */
    def readNode(path: String): Array[Byte] = {
      zk.getData(path, true, zk.exists(path, true))
    }

    /**
      * Save Node
      * @param path
      * @param data
      */
    def saveNode(path: String, data: Array[Byte]): Unit = {
      if (existNode(path) != null)
        zk.setData(path, data, zk.exists(path, true).getVersion)
      else
        zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
    }

    /**
      * Remove Node
      * @param path
      */
    def removeNode(path: String): Unit = {
      zk.delete(path, zk.exists(path, true).getVersion)
    }

    /**
      * Lode Node
      * @param path
      * @return
      */
    def loadNode(path: String): Array[Byte] = {
      zk.getData(path, false, null)
    }

    // saveNode zk.create or zk.setData
    // removeNode zk.delete
    // loadNode zk.getData


    /**
      * Read Offsets stored in the store
      * @param streamPath
      * @return
      */
    def readOffsets(topic: String): Map[String, String] = {
      val offsets = collection.mutable.HashMap.empty[String, String]
      val stat=zk.exists(lPath, true)
      if(stat != null) {
        val zNodes = zk.getChildren(lPath, true).asScala
        zNodes.map {zNode => offsets.put(zNode, new String(loadNode(lPath+"/"+zNode)))}
        //println(zNodes.map(_.productIterator.mkString(":")).mkString("|"))
      }
      offsets.toMap
    }

    /**
      * Save Offsets stored to the store
      * @param streamPath
      * @return
      */
    def saveOffsets(topic: String, offsets: Map[String, String]): Unit = {
      offsets.map(f => {saveNode(lPath+"/"+f._1,f._2.toString.getBytes)})
    }

    /**
      * Read offsets when exists otherwise return configured offset for startingOffsets in properties
      * @param topics
      * @param groupId
      * @param defaultValue
      * @return
      */
    def readOffsets(topics: Seq[String], groupId: String, defaultValue: String): String = {
      //
      var offsetValue = defaultValue

      val suite = scala.collection.mutable.Map[String, Map[String, Long]]()
      val partitionMap = zkUtils.getPartitionAssignmentForTopics(topics)

      // /consumers/<groupId>/offsets/<topic>/
      partitionMap.foreach(topicPartitions => {

        //
        val topic = topicPartitions._1
        val zkGroupTopicDirs = new ZKGroupTopicDirs(groupId,topic)
        val offsets = scala.collection.mutable.Map[String, Long]()

        //
        topicPartitions._2.foreach(partition => {

          //
          val offsetPath = zkGroupTopicDirs.consumerOffsetDir+"/"+partition

          //
          val offsetStartTuple = zkUtils.readData(offsetPath)
          if (offsetStartTuple ne null) {
            logger.warn("retrieving offset details - topic: {}, partition {}, offset: {}, node path: {}" + topicPartitions._1 + " " + partition.toString() + " " + offsetStartTuple._1 + " " + offsetPath)
            offsets += (partition.toString() -> offsetStartTuple._1.toLong)
          }

        })
        //
        suite += (topic -> offsets.toMap)
      })
      val offsetSuite = OffsetSuite(suite.toMap)

      //
      logger.info("topicPartOffsetMap.topMap " + suite.toMap)
      implicit val formats = Serialization.formats(NoTypeHints)

      try {
        val trim: Regex = """[{]"suite":(.*)[}]""".r //raw"[,\s]*(|.*[^,\s])[,\s]*".r
        offsetValue = write(offsetSuite) match { case trim(x) => x }
      } catch {
        case t: Throwable => t.printStackTrace()
      }

      //
      offsetValue
    }


    /**
      * Save offsets to zookeeper path
      * @param offsets
      * @param groupId
      */
    def saveOffsets(offsets: String, groupId: String): Unit = {
      //logger.info("saveOffsets: "+offsets)
      implicit val formats = DefaultFormats

      //
      val offsetSuite = parse("{\"suite\":"+offsets+"}").extract[OffsetSuite]

      //
      offsetSuite.suite.map( topicOS => {

        //
        val topic = topicOS._1
        val offsets = topicOS._2

        //
        offsets.map(f=> {
          val zkGroupTopicDirs = new ZKGroupTopicDirs(groupId, topic)

          val acls = new ListBuffer[ACL]()
          val acl = new ACL
          acl.setId(Ids.ANYONE_ID_UNSAFE)
          acl.setPerms(Perms.ALL)
          acls += acl

          //
          val offsetPath = zkGroupTopicDirs.consumerOffsetDir+"/"+f._1
          val offsetVal = f._2
          zkUtils.updatePersistentPath(zkGroupTopicDirs.consumerOffsetDir+"/"+f._1,offsetVal+"",acls.asJava)

          logger.info("persisting offset details - topic: {}, partition {}, offset: {}, node path: {} "+topic+" "+f._1+" "+offsetVal.toString+" "+offsetPath)


        })

      })



    }



  }

  /**
    * A Builder class to collect required configs related to persistent store
    */
  class Builder extends BaseTrait {

    val options = new scala.collection.mutable.HashMap[String, String]

    /**
      * config option
      */
    def config(key: String, value: String): Builder = synchronized {
      options += key -> value
      this
    }

    /**
      * config option
      */
    def config(key: String, value: Long): Builder = synchronized {
      options += key -> value.toString
      this
    }

    /**
      * config option
      */
    def config(key: String, value: Double): Builder = synchronized {
      options += key -> value.toString
      this
    }

    /**
      * config option
      */
    def config(key: String, value: Boolean): Builder = synchronized {
      options += key -> value.toString
      this
    }

    /**
      * config option
      */
    def config(conf: Map[String, String]): Builder = synchronized {
      conf.foreach { case (k, v) => options += k -> v }
      this
    }

    /**
      * Get or Create OffsetStore
      * @param storeType
      * @return
      */
    def getOrCreate(storeType: String): OffsetStore = {

      //
      if (offsetStore eq null) {

        //
        offsetStore = storeType match {
          case "zookeeper" => {
            // /consumers/<groupid>/offsets/<topic>
            var basePath = options.getOrElse("zkBasePath","")
            var zkHost = options.getOrElse("zkHosts","")
            val zkOStore = new ZookeeperOffsetStore()
            zkOStore.initialize(zkHost, basePath)
            offsetStore = zkOStore
            offsetStore
          }
          case _ => null
        }

      }


      //
      offsetStore
    }

  }

  /**
    * Creates builder
    * @return
    */
  def builder(): Builder = {storeBuilder = new Builder(); storeBuilder}


  /**
    * Returns current builder
    * @return
    */
  def getBuilder(): Builder = storeBuilder


  /**
    * Returns OffsetStore instance
    * @return
    */
  def getOffsetStore(): OffsetStore = offsetStore


}
