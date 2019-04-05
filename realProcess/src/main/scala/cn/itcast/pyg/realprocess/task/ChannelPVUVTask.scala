package cn.itcast.pyg.realprocess.task

import cn.itcast.pyg.realprocess.`trait`.DataProcess
import cn.itcast.pyg.realprocess.bean.{ChannelPVUV, Message}
import cn.itcast.pyg.realprocess.map.ChannelPVUVFlatMap
import cn.itcast.pyg.realprocess.reduce.ChannelPVUVReduce
import cn.itcast.pyg.realprocess.sink.ChannelPVUVSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object ChannelPVUVTask extends DataProcess{
  override def process(source: DataStream[Message]): Unit = {
    //1.类型转换,将Message-> ChannelPVUV对象
    val flatMapStream: DataStream[ChannelPVUV] = source.flatMap(new ChannelPVUVFlatMap)
    //2. 分组
    val groupStream: KeyedStream[ChannelPVUV, String] = flatMapStream.keyBy(line => line.aggregateField)
    //3. 划分时间窗口
    val window: WindowedStream[ChannelPVUV, String, TimeWindow] = groupStream.timeWindow(Time.seconds(5))
    //4. 聚合操作
    val reduceStream: DataStream[ChannelPVUV] = window.reduce(new ChannelPVUVReduce)

    //5. 数据落地
    reduceStream.addSink(new ChannelPVUVSink)

  }
}
