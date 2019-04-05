package cn.itcast.pyg.realprocess

import java.lang
import java.util.Properties

import cn.itcast.pyg.realprocess.bean.{Message, UserBean}
import cn.itcast.pyg.realprocess.task._
import cn.itcast.pyg.realprocess.tools.KafkaConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.flink.streaming.util.serialization.SimpleStringSchema



/**
  * Flink程序的入口程序
  */
object App {

  def main(args: Array[String]): Unit = {
    //使用Flink获取Kafka的消息,进行业务处理

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置水印处理时机
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //开启Checkpoint机制.设置每隔5S做一次
    env.enableCheckpointing(5000)
    //设置备份模式为仅一次
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //设置超时时间,预防满磁盘
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    //设置两次checkpoint最小间隔时间
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000)
    //设置每次有几个做checkpoint
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    //设置当checkpoint失败后的处理策略,false:丢弃
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)

    env.setStateBackend(new FsStateBackend("hdfs://node01:8020/pyg-chk00000000"))

    //并行度设置为1
    env.setParallelism(1)

    //env加载数据源?
    //数据源是谁?Kafka
    val consumer: FlinkKafkaConsumer09[String] = new FlinkKafkaConsumer09[String](
      KafkaConfig.KAFKA_TOPIC,
      new SimpleStringSchema(),
      KafkaConfig.getProperties
    )

    //添加数据源
    val source: DataStream[String] = env.addSource(consumer)

    //json=>bean
    val messageSource = source.map(new MapFunction[String, Message] {
      override def map(json: String): Message = {
        val jsonObj: JSONObject = JSON.parseObject(json)
        val timeStamp: lang.Long = jsonObj.getLong("timeStamp")
        val count: Integer = jsonObj.getInteger("count")
        val dataStr: String = jsonObj.getString("data")

        val userBean: UserBean = UserBean.toBean(dataStr)
        Message(timeStamp, count, userBean)
      }
    })
//    Message(1553560895403,1,{"browserType":"火狐","categoryID":6,"channelID":4,"city":"上海","country":"America","entryTime":1544605260000,"leaveTime":1544634060000,"network":"移动","produceID":2,"province":"china","source":"必应跳转","userID":5})
//    Message(1553561703319,1,UserBean(qq浏览器,20,17,北京,china,1544619660000,1544634060000,电信,16,America,必应跳转,16))

    //指定水印的处理策略
    val waterStream = messageSource.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[Message] {

      //当前时间戳
      var currentTimeStamp = 0L
      //最大延迟
      var maxDelay = 2000L
      //水印对象
      var watermark: Watermark = null

      //得到当前水印
      override def getCurrentWatermark: Watermark = {
        watermark = new Watermark(currentTimeStamp - maxDelay)
        watermark
      }

      //抽取时间戳
      override def extractTimestamp(msg: Message, previousElementTimestamp: Long): Long = {
        var time = msg.timeStamp
        currentTimeStamp = time
//        println(time + "|" + watermark.getTimestamp)
        currentTimeStamp
      }
    })

    waterStream.print()

    //实时热度
//    ChannelRealHotTask.process(waterStream)
    //PVUV统计
//    ChannelPVUVTask.process(waterStream)
    //用户新鲜度
//    ChannelUserFreshTask.process(waterStream)
    //区域统计
//    ChannelRegionTask.process(waterStream)
    //浏览器
//    UserBrowserTask.process(waterStream)
    //用户网络统计
    UserNetWorkTask.process(waterStream)


    env.execute("pyg")
  }
}
