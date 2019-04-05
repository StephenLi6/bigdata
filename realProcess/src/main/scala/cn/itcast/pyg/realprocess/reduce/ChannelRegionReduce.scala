package cn.itcast.pyg.realprocess.reduce

import cn.itcast.pyg.realprocess.bean.ChannelRegion
import org.apache.flink.api.common.functions.ReduceFunction

class ChannelRegionReduce extends ReduceFunction[ChannelRegion]{
  override def reduce(oldData: ChannelRegion, newData: ChannelRegion): ChannelRegion = {
    val region = new ChannelRegion
    region.channelID = oldData.channelID

    //区域相关字段
    region.country = oldData.country
    region.province = oldData.province
    region.city = oldData.city
    //新老用户
    region.newCount = oldData.newCount + newData.newCount
    region.oldCount = oldData.oldCount + newData.oldCount
    //PVUV
    region.pv = oldData.pv + newData.pv
    region.uv = oldData.uv + newData.uv

    region.dataField = oldData.dataField
    region.aggregateField = oldData.aggregateField

    region
  }
}
