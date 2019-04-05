package cn.itcast.pyg.realprocess.map

import cn.itcast.pyg.realprocess.bean.{ChannelRealHot, Message}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector


class ChannelRealHotFlatMap extends FlatMapFunction[Message, ChannelRealHot]{

  /**
    *
    * @param value 原来的数据
    * @param out 采集器,用于收集处理过的数据
    */
  override def flatMap(msg: Message, out: Collector[ChannelRealHot]): Unit = {
    val hot = new ChannelRealHot
    //封装数据
    hot.channelID = msg.userBean.channelID
    hot.count = msg.count
    hot.timeStamp = msg.timeStamp

    //采集数据
    out.collect(hot)
  }
}
