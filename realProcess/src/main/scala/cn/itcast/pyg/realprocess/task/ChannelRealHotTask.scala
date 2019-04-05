package cn.itcast.pyg.realprocess.task

import cn.itcast.pyg.realprocess.`trait`.DataProcess
import cn.itcast.pyg.realprocess.bean.{ChannelRealHot, Message}
import cn.itcast.pyg.realprocess.map.ChannelRealHotFlatMap
import cn.itcast.pyg.realprocess.reduce.ChannelRealHotReduce
import cn.itcast.pyg.realprocess.sink.ChannelRealHotSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object ChannelRealHotTask extends DataProcess{
  //处理实时热度相关流程
  override def process(source: DataStream[Message]): Unit = {
    //业务流程
    //1. 数据转换message->ChannelRealHot
    val flatMapStream: DataStream[ChannelRealHot] = source.flatMap(new ChannelRealHotFlatMap)
    //2. 分组
    val groupStream: KeyedStream[ChannelRealHot, String] = flatMapStream.keyBy(line => line.channelID)
    //3. 窗口划分
    val window: WindowedStream[ChannelRealHot, String, TimeWindow] = groupStream.timeWindow(Time.seconds(5))
    //4. 聚合操作
    val reduceStream: DataStream[ChannelRealHot] = window.reduce(new ChannelRealHotReduce)
    //5. 数据落地
    reduceStream.addSink(new ChannelRealHotSink)
  }
}
