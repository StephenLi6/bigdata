package com.theshy

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/*
* The Best Or Nothing
* Desinger:TheShy
* Date:2019/3/514:40
* com.theshybigdata
*/
object SparkStreamingSoccketTotal {

  def updateFunction (newValues:Seq[Int] ,runningCount: Option[Int]) : Option[Int] = {
    val newCount = runningCount.getOrElse(0) +newValues.sum
    Some(newCount)
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkStreamingSoccketTotal").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc: StreamingContext = new StreamingContext(sc,Seconds(5))
    ssc.checkpoint("./ck")
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("node1",9999)
    val result: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_,1)).updateStateByKey(updateFunction)
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
