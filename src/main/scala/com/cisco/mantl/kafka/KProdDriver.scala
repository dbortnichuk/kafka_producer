package com.cisco.mantl.kafka

import java.util.concurrent.Executors

import scala.concurrent._
import java.io.{InputStreamReader, BufferedReader}
import java.util._
import org.apache.hadoop.fs._
import org.apache.hadoop.conf._
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

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
      opt[Int]('t', "threads") valueName ("<threads>") action { (x, c) =>
        c.copy(threads = x)
      } text (MsgThreads)
      help("help") text (MsgHelp)
      note(MsgNote)
    }

    parser.parse(args, Config()) match {
      case Some(config) =>

        implicit val ec = new ExecutionContext {
          val threadPool = Executors.newFixedThreadPool(config.threads);

          override def reportFailure(cause: Throwable): Unit = {};

          override def execute(runnable: Runnable): Unit = threadPool.submit(runnable);

          def shutdown() = threadPool.shutdown();
        }

        val props = new Properties()
        props.load(getClass.getResourceAsStream("/producer-defaults.properties"))
        props.put("metadata.broker.list", config.brokers)
        val pConfig = new ProducerConfig(props)

        val fs = FileSystem.get(new Configuration())
        val status = fs.listStatus(new Path(config.inputDir)) //"hdfs://quickstart.cloudera:8020/user/examples1/files"

        status.foreach(st => ec.execute(readAndForward(fs, st.getPath)))

        ec.shutdown()

        def readAndForward(fs: FileSystem, path: Path): Unit = {
          val producer = new Producer[String, String](pConfig)
          val br = new BufferedReader(new InputStreamReader(fs.open(path)))
          var line = br.readLine()
          while (line != null) {
            System.out.println(line)
            producer.send(new KeyedMessage[String, String](config.topic, line, line))
            line = br.readLine()
          }
          producer.close();
        }

      case None => println("ERROR: bad argument set provided")
    }

  }

  case class Config(brokers: String = "",
                    topic: String = "",
                    inputDir: String = "",
                    threads: Int = 1)

}


