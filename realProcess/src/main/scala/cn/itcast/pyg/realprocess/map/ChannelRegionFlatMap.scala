package cn.itcast.pyg.realprocess.map

import cn.itcast.pyg.realprocess.bean.{ChannelRegion, Message}
import cn.itcast.pyg.realprocess.tools.{TimeUtils, UserState, UserStateUtils}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector

class ChannelRegionFlatMap extends FlatMapFunction[Message, ChannelRegion] {
  override def flatMap(msg: Message, out: Collector[ChannelRegion]): Unit = {


    val region = new ChannelRegion

    region.channelID = msg.userBean.channelID

    //区域相关
    region.country = msg.userBean.country
    region.province = msg.userBean.province
    region.city = msg.userBean.city

    region.pv = 1L


    val hour: String = TimeUtils.getTime(msg.timeStamp.toString, TimeUtils.HOUR_PATTERN)
    val day: String = TimeUtils.getTime(msg.timeStamp.toString, TimeUtils.DAY_PATTERN)
    val month: String = TimeUtils.getTime(msg.timeStamp.toString, TimeUtils.MONTH_PATTERN)


    //使用用户ID+频道ID+国家+省份+城市作为rowkey
    val userStateRowKey =
      msg.userBean.userID + ":" +
      region.channelID + ":" +
      region.country + ":" +
      region.province + ":" +
      region.city
    val userState: UserState = UserStateUtils.getUserState(userStateRowKey, msg.timeStamp.toString)

    //新老用户
    if (userState.isNew) {
      //是新用户
      region.newCount = 1L
    } else {
      //是老用户
      region.newCount = 0L
    }

    //如果是这个小时新来的
    if (userState.isHour) {
      //老用户+1
      region.oldCount = 1L
      //uv+1
      region.uv = 1L
    } else {
      region.oldCount = 0L
      region.uv = 0L
    }
    region.dataField = region.channelID + ":" + region.country + ":" + region.province + ":" + region.city + ":" + hour
    region.aggregateField = region.channelID + ":" + region.country + ":" + region.province + ":" + region.city + ":" + hour

    out.collect(region)

    //如果是今天新来的
    if (userState.isHour) {
      //老用户+1
      region.oldCount = 1L
      //uv+1
      region.uv = 1L
    } else {
      region.oldCount = 0L
      region.uv = 0L
    }
    region.dataField = region.channelID + ":" + region.country + ":" + region.province + ":" + region.city + ":" + day
    region.aggregateField = region.channelID + ":" + region.country + ":" + region.province + ":" + region.city + ":" + day

    out.collect(region)

    //如果是这个月新来的
    if (userState.isHour) {
      //老用户+1
      region.oldCount = 1L
      //uv+1
      region.uv = 1L
    } else {
      region.oldCount = 0L
      region.uv = 0L
    }
    region.dataField = region.channelID + ":" + region.country + ":" + region.province + ":" + region.city + ":" + month
    region.aggregateField = region.channelID + ":" + region.country + ":" + region.province + ":" + region.city + ":" + month

    out.collect(region)

  }
}
