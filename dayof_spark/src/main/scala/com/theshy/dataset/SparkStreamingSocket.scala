package com.theshy.dataset

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}

/*
* The Best Or Nothing
* Desinger:TheShy
* Date:2019/3/511:30
* com.theshybigdata
*/
object SparkStreamingSocket extends App {
  private val conf: SparkConf = new SparkConf().setAppName("SparkSteamingSocket").setMaster("local[2]")
  val sc: SparkContext = new SparkContext(conf)
  val ssc = new StreamingContext(sc,Seconds(5))
  val lines: ReceiverInputDStream[String] = ssc.socketTextStream("node1",9999)
  val result: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

  result.print()
  ssc.start()
  ssc.awaitTermination()
}
