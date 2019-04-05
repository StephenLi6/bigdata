package cn.itcast.pyg.realprocess.reduce

import cn.itcast.pyg.realprocess.bean.ChannelUserFresh
import org.apache.flink.api.common.functions.ReduceFunction

class ChannelUserFreshReduce extends ReduceFunction[ChannelUserFresh]{
  override def reduce(oldData: ChannelUserFresh, newData: ChannelUserFresh): ChannelUserFresh = {
    val fresh = new ChannelUserFresh
    fresh.channelID = oldData.channelID
    fresh.userID = oldData.userID
    fresh.timeStamp = oldData.timeStamp
    fresh.newCount = oldData.newCount + newData.newCount
    fresh.oldCount = oldData.oldCount + oldData.oldCount

    fresh.dataField = oldData.dataField
    fresh.aggregateField = oldData.aggregateField

    fresh
  }
}
