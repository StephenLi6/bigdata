package cn.itcast.pyg.realprocess.map

import cn.itcast.pyg.realprocess.bean.{ChannelUserFresh, Message}
import cn.itcast.pyg.realprocess.tools.{TimeUtils, UserState, UserStateUtils}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector

class ChannelUserFreshFlatMap extends FlatMapFunction[Message, ChannelUserFresh]{
  override def flatMap(msg: Message, out: Collector[ChannelUserFresh]): Unit = {
    val fresh = new ChannelUserFresh

    fresh.channelID = msg.userBean.channelID
    fresh.userID = msg.userBean.userID
    fresh.timeStamp = msg.timeStamp


    val hour: String = TimeUtils.getTime(fresh.timeStamp.toString, TimeUtils.HOUR_PATTERN)
    val day: String = TimeUtils.getTime(fresh.timeStamp.toString, TimeUtils.DAY_PATTERN)
    val month: String = TimeUtils.getTime(fresh.timeStamp.toString, TimeUtils.MONTH_PATTERN)

    //看是不是新用户?
    val userState: UserState = UserStateUtils.getUserState(fresh.userID + ":" + fresh.channelID, fresh.timeStamp.toString)

    if (userState.isNew) {
      //是新用户
      fresh.newCount = 1L
    } else {
      //不是新用户
      fresh.newCount = 0L
    }

    //设置oldcount
    //是否是这个小时第一次访问
    if (userState.isHour) {
      fresh.oldCount = 1L
    } else {
      fresh.oldCount = 0L
    }
    //    channelid:201903    newcount 3   oldcount 6
    fresh.dataField = fresh.channelID + ":" + hour
    fresh.aggregateField = fresh.channelID + ":" + hour

    out.collect(fresh) //采集小时的数据

    //是否是这一天第一次访问
    if (userState.isDay) {
      fresh.oldCount = 1L
    } else {
      fresh.oldCount = 0L
    }
    //    channelid:201903    newcount 3   oldcount 6
    fresh.dataField = fresh.channelID + ":" + day
    fresh.aggregateField = fresh.channelID + ":" + day

    out.collect(fresh) //采集这一天的数据

    //是否是这一个月第一次访问
    if (userState.isMonth) {
      fresh.oldCount = 1L
    } else {
      fresh.oldCount = 0L
    }
    //    channelid:201903    newcount 3   oldcount 6
    fresh.dataField = fresh.channelID + ":" + month
    fresh.aggregateField = fresh.channelID + ":" + month

    out.collect(fresh) //采集这个月的数据

  }

}
