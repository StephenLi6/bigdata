package com.theshy.dataset

/*
* The Best Or Nothing
* Desinger:TheShy
* Date:2019/3/1118:33
* com.theshybigdata
*/
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
object StreamingKafka1 {
  def main(args: Array[String]): Unit = {
    val sparkContext: SparkContext = new SparkContext(new SparkConf().set("spark.streaming.backpressure.enabled","true")
        .set("spark.streaming.stopGracefullyOnShutdown","true")
      .setMaster("local[4]").setAppName("sparkstreaming+kafka"))
    sparkContext.setLogLevel("WARN")
    val streamingContext: StreamingContext = new StreamingContext(sparkContext,Seconds(1))
    val brokers = "node1:9092,node2:9092,node3:9092"
    val sourceTopic = "order"
    val group = "sparkKafkaGroup"
    val kafkaParam = Map(
      "bootstrap.servers" -> brokers,//用于初始化链接到集群的地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      //用于标识这个消费者属于哪个消费团体
      "group.id" -> group,
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      //可以使用这个配置，latest自动重置偏移量为最新的偏移量
      "auto.offset.reset" -> "latest",
      //如果是true，则这个消费者的偏移量会在后台自动提交
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val directStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(streamingContext, locationStrategy = LocationStrategies.PreferConsistent,
      consumerStrategy = ConsumerStrategies.Subscribe[String,String](Array("order"), kafkaParam))
    directStream.foreachRDD(x=>{
      if(x.count() > 0){
        x.foreach(f =>{
          val value :String = f.value()
          println(value)
          Thread.sleep(10000)
        })
        val offsetRange: Array[OffsetRange] = x.asInstanceOf[HasOffsetRanges].offsetRanges
        directStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRange)
      }
    })

    streamingContext.start()
    streamingContext.awaitTermination()

   /* "spark.streaming.receiver.maxRate"-> "1000",
    "spark.streaming.kafka.maxRatePerPartition"-> "400",
    "spark.streaming.backpressure.initialRate "-> "1000",*/

  }
}

