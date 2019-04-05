package cn.itcast.pyg.realprocess.task

import cn.itcast.pyg.realprocess.`trait`.DataProcess
import cn.itcast.pyg.realprocess.bean.Message
import cn.itcast.pyg.realprocess.map.ChannelRegionFlatMap
import cn.itcast.pyg.realprocess.reduce.ChannelRegionReduce
import cn.itcast.pyg.realprocess.sink.ChannelRegionSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object ChannelRegionTask extends DataProcess{
  override def process(source: DataStream[Message]): Unit = {

    //类型转换
    source.flatMap(new ChannelRegionFlatMap)
    //分组
      .keyBy(line => line.aggregateField)
    //时间窗口划分
      .timeWindow(Time.seconds(5))
    //聚合操作
      .reduce(new ChannelRegionReduce)
    //数据落地
      .addSink(new ChannelRegionSink)
  }
}
