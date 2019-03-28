package com.theshy.dataset

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/*
* The Best Or Nothing
* Desinger:TheShy
* Date:2019/3/521:38
* com.theshybigdata
*/
object SparkStreamingSocketWindow {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("SparkStreamingSocketWindow").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    val ssc: StreamingContext = new StreamingContext(sc,Seconds(5))
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("node1",9999)
    val result: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_,1)).reduceByKeyAndWindow((x:Int,y:Int)=> x+y ,Seconds(5),Seconds(5))
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
