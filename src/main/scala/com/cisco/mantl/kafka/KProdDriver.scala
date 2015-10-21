package com.cisco.mantl.kafka

import java.util.concurrent.Executors

import kafka.admin.AdminUtils
import kafka.api.TopicMetadata
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.serialize.ZkSerializer

import scala.concurrent._
import java.io.{InputStreamReader, BufferedReader}
import java.util._
import org.apache.hadoop.fs._
import org.apache.hadoop.conf._
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.utils.ZkUtils._

/**
 * Created by dbort on 20.10.2015.
 */
object KProdDriver {

  def main(args: Array[String]) {

    val parser = new scopt.OptionParser[Config](MsgUsage) {
      head("Kafka Producer", "1.0")
      opt[String]('b', "brokers") required() valueName ("<brokers>") action { (x, c) =>
        c.copy(brokers = x.trim)
      } text (MsgBrokers)
      opt[String]('t', "topic") required() valueName ("<topic>") action { (x, c) =>
        c.copy(topic = x.trim)
      } text (MsgTopic)
      opt[String]('i', "inputDir") required() valueName ("<inputDir>") action { (x, c) =>
        c.copy(inputDir = x.trim)
      } text (MsgInputDir)
      opt[String]('z', "zkhost") valueName ("<zkHost>") action { (x, c) =>
        c.copy(zkHost = x)
      } text (MsgZookeeper)
      help("help") text (MsgHelp)
      note(MsgNote)
    }

    parser.parse(args, Config()) match {
      case Some(config) =>

        val zkClient = new ZkClient(config.zkHost, 10000, 10000);
        zkClient.setZkSerializer(getZkSerializer)

        val metaData: TopicMetadata = AdminUtils.fetchTopicMetadataFromZk(config.topic, zkClient);
        val partitionNum = metaData.partitionsMetadata.size

        println(String.format("%s : %s partitions found for %s topic. Setting appropriate file reading pool size.",
          getClass.getCanonicalName, partitionNum.toString, config.topic));

        implicit val ec = new ExecutionContext {
          val threadPool = Executors.newFixedThreadPool(partitionNum);

          override def reportFailure(cause: Throwable): Unit = {};

          override def execute(runnable: Runnable): Unit = threadPool.submit(runnable);

          def shutdown() = threadPool.shutdown();
        }

        val producerConfig = getProducerConfig(config.brokers)

        val fs = FileSystem.get(new Configuration())
        val status = fs.listStatus(new Path(config.inputDir)) //"hdfs://quickstart.cloudera:8020/user/examples1/files"

        status.foreach(st => ec.execute(readAndForward(fs, st.getPath)))

        ec.shutdown()

        def readAndForward(fs: FileSystem, path: Path): Unit = {
          val producer = new Producer[String, String](producerConfig)
          val br = new BufferedReader(new InputStreamReader(fs.open(path)))
          var line = br.readLine()
          while (line != null) {
            //System.out.println(line)
            producer.send(new KeyedMessage[String, String](config.topic, line.hashCode.toString, appendPrefix(line)))
            line = br.readLine()
          }
          producer.close();
        }

      case None => println("ERROR: bad argument set provided")
    }

    def getProducerConfig(brokers: String): ProducerConfig ={
      val props = new Properties()
      props.load(getClass.getResourceAsStream("/producer-defaults.properties"))
      props.put("metadata.broker.list", brokers)
      new ProducerConfig(props)
    }

    def getZkSerializer(): ZkSerializer = new ZkSerializer {
      override def serialize(data: scala.Any): Array[Byte] = ZKStringSerializer.serialize(data.asInstanceOf[java.lang.Object])

      override def deserialize(bytes: Array[Byte]): AnyRef = ZKStringSerializer.deserialize(bytes)
    }

    def appendPrefix(line: String): String = {
      String.format("Thread-%s;Time-%s;Line-%s", Thread.currentThread().getId.toString, System.nanoTime().toString, line)
    }

  }

  case class Config(brokers: String = "",
                    topic: String = "",
                    inputDir: String = "",
                    zkHost: String = "localhost:2181")

}


