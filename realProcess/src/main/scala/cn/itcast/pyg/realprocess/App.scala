package cn.itcast.pyg.realprocess

import java.lang
import java.util.Properties

import cn.itcast.pyg.realprocess.bean.Message
import cn.itcast.pyg.realprocess.tools.KafkaConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

/**
  * Flink程序的入口程序
  */
object App {

  def main(args: Array[String]): Unit = {
    //使用Flink获取Kafka的消息,进行业务处理

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //env加载数据源?
    //数据源是谁?Kafka
    val consumer = new FlinkKafkaConsumer09[String](
      KafkaConfig.KAFKA_TOPIC,
      new SimpleStringSchema(),
      KafkaConfig.getProperties
    )

    val source: DataStreamSource[String] = env.addSource(consumer)

    val messageSource = source.map(new MapFunction[String, Message] {
      override def map(json: String): Message = {
        val jsonObj: JSONObject = JSON.parseObject(json)
        val timeStamp: lang.Long = jsonObj.getLong("timeStamp")
        val count: Integer = jsonObj.getInteger("count")
        val dataStr: String = jsonObj.getString("data")
        Message(timeStamp, count, dataStr)
      }
    })

    messageSource.print()

    env.execute("pyg")
  }
}
