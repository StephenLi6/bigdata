package cn.itcast.pyg.realprocess.task

import cn.itcast.pyg.realprocess.`trait`.DataProcess
import cn.itcast.pyg.realprocess.bean.Message
import cn.itcast.pyg.realprocess.map.ChannelUserFreshFlatMap
import cn.itcast.pyg.realprocess.reduce.ChannelUserFreshReduce
import cn.itcast.pyg.realprocess.sink.ChannelUserFreshSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object ChannelUserFreshTask extends DataProcess{
  override def process(source: DataStream[Message]): Unit = {
    //类型转换,message->ChannelUserFresh
    source.flatMap(new ChannelUserFreshFlatMap)
    //分组
      .keyBy(line => line.aggregateField)
    //窗口划分
      .timeWindow(Time.seconds(5))
    //聚合操作
      .reduce(new ChannelUserFreshReduce)
    //数据落地
      .addSink(new ChannelUserFreshSink)

  }
}
