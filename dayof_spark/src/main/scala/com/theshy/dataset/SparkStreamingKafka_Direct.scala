package com.theshy.dataset

/*import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}*/

/*
* The Best Or Nothing
* Desinger:TheShy
* Date:2019/3/1012:05
* com.theshybigdata
*/
object SparkStreamingKafka_Direct {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("SparkStreamingKafka_Direct").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    val ssc: StreamingContext = new StreamingContext(sc,Seconds(5))
    ssc.checkpoint("./Kafka_Direct")
    val dstream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[StringDecoder,StringDecoder](ssc,kafkaParams = Map("metadata.broker.list"->"node1:9092,node2:9092,node3:9092","group.id"->"Kafka_Direct"),topics = Set("order"))
    dstream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      // some time later, after outputs have completed
      dstream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }
    dstream.map(_._2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()
    ssc.start()
    ssc.awaitTermination()


  }
}
