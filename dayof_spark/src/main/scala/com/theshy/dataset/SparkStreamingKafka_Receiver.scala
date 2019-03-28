package com.theshy.dataset

/*import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable*/

/*
* The Best Or Nothing
* Desinger:TheShy
* Date:2019/3/912:51
* com.theshybigdata
*/
object SparkStreamingKafka_Receiver {
  def main(args: Array[String]): Unit = {
    /*val conf: SparkConf = new SparkConf().setAppName("SparkStreamingKafka_Receiver").setMaster("local[4]")
      .set("spark.streaming.receiver.writeAheadLog.enable", "true")
    val sc: SparkContext = new SparkContext(conf)
    val context: StreamingContext = new StreamingContext(sc,Seconds(5))
    context.checkpoint("./Kafka_Receiver")

    val zkQuorum = "node1:2181,node2:2181,node3:2181"
    val groupId = "spark_receiver1"
    val topics = Map("order"->2)

    val receiverDstream: immutable.IndexedSeq[ReceiverInputDStream[(String, String)]] = (1 to 3).map(x => {
      val stream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(context, zkQuorum, groupId, topics)
      stream
    })

    val unionDStream: DStream[(String, String)] = context.union(receiverDstream)
    val result: DStream[(String, Int)] = unionDStream.map(_._2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    result.print()
    context.start()
    context.awaitTermination()*/
  }
}
