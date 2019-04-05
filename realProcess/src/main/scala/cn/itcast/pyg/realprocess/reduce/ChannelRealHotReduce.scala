package cn.itcast.pyg.realprocess.reduce

import cn.itcast.pyg.realprocess.bean.ChannelRealHot
import org.apache.flink.api.common.functions.ReduceFunction

class ChannelRealHotReduce extends ReduceFunction[ChannelRealHot]{

  /*
12->3
    12->2

    12->5
    12->1

    12->6
    */
  /**
    * 要进行的聚合操作
    * @param oldData 老的数据
    * @param newData 新的数据
    * @return
    */
  override def reduce(oldData: ChannelRealHot, newData: ChannelRealHot): ChannelRealHot = {

    val realHot = new ChannelRealHot

    realHot.channelID = newData.channelID

    realHot.timeStamp = newData.timeStamp

    //老的count+新的count
    realHot.count = oldData.count + newData.count
    //返回计算后的realHot
    realHot
  }
}
