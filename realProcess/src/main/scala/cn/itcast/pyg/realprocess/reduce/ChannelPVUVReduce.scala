package cn.itcast.pyg.realprocess.reduce

import cn.itcast.pyg.realprocess.bean.ChannelPVUV
import org.apache.flink.api.common.functions.ReduceFunction

class ChannelPVUVReduce extends ReduceFunction[ChannelPVUV]{

  override def reduce(oldPVUV: ChannelPVUV, newPVUV: ChannelPVUV): ChannelPVUV = {
    val pvuv = new ChannelPVUV
    pvuv.channelID = oldPVUV.channelID
    pvuv.userID = oldPVUV.userID
    pvuv.timeStamp = oldPVUV.timeStamp
    pvuv.dataField = oldPVUV.dataField
    pvuv.aggregateField = oldPVUV.aggregateField

    //将新老数据的pv/uv进行累加操作
    pvuv.pv = oldPVUV.pv + newPVUV.pv
    pvuv.uv = oldPVUV.uv + newPVUV.uv

    pvuv
  }
}
