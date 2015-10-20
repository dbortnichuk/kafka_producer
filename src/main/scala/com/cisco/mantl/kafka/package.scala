package com.cisco.mantl

import java.util.concurrent.Executors

import scala.concurrent.ExecutionContext

/**
 * Created by dbort on 20.10.2015.
 */
package object kafka {

  val MsgUsage = "spark-submit --class \"com.cisco.mantl.kafka.KProdDriver\" *.jar"
  val MsgBrokers = "\"brokers\" is a required property, specify it as comma separated list to point out brokers Kafka stream to be created on, example: broker1_host:port,broker2_host:port"
  val MsgTopic = "\"topic\" is a required property, specify it to point out topic to send to, example: topic1"
  val MsgInputDir = "\"index\" is a required property, specify it to point out root directory files should be read from, example: hdfs://quickstart.cloudera:8020/user/examples/"
  val MsgThreads = "\"threads\" is an optional property, defines number of threads will be reading files from input directory, default: 1"
  val MsgHelp = "Use this option to check application usage"
  val MsgNote = "NOTE: arguments with spaces should be enclosed in \" \"."

  implicit def convertToRunnable[F](f: => F) = new Runnable() {
    def run() {
      f
    }
  }

}
