package cn.itcast.pyg.realprocess.map

import cn.itcast.pyg.realprocess.bean.{ChannelPVUV, Message}
import cn.itcast.pyg.realprocess.tools.{TimeUtils, UserState, UserStateUtils}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector

class ChannelPVUVFlatMap extends FlatMapFunction[Message, ChannelPVUV]{
  override def flatMap(msg: Message, out: Collector[ChannelPVUV]): Unit = {

    val pvuv = new ChannelPVUV
    //将msg 转换为 pvuv对象
    val channelID = msg.userBean.channelID
    val userID = msg.userBean.userID
    val timeStamp = msg.timeStamp
    //进来一次数据,就是点击了一次,将pv设为1
    val pv = 1L
    var uv = 0L

    var dataField = ""
    var aggregateField = ""

    val hour: String = TimeUtils.getTime(timeStamp.toString, TimeUtils.HOUR_PATTERN)
    val day: String = TimeUtils.getTime(timeStamp.toString, TimeUtils.DAY_PATTERN)
    val month: String = TimeUtils.getTime(timeStamp.toString, TimeUtils.MONTH_PATTERN)


    //如何知道uv的数量?
    //uv和时间挂钩

    val userState: UserState = UserStateUtils.getUserState(userID + ":" + channelID, timeStamp.toString)

    if (userState.isHour) {
      //是这个小时第一次过来
      uv = 1L
      dataField = channelID + ":" + hour
      aggregateField = channelID + ":" + hour
    } else {
      uv = 0L
      dataField = channelID + ":" + hour
      aggregateField = channelID + ":" + hour
    }
    pvuv.channelID = channelID
    pvuv.userID = userID
    pvuv.pv = pv
    pvuv.uv = uv
    pvuv.dataField = dataField
    pvuv.aggregateField = aggregateField
    out.collect(pvuv)
    /*=================小时的PVUV采集完毕=================*/

    if (userState.isDay) {
      //是这一天第一次过来
      uv = 1L
      dataField = channelID + ":" + day
      aggregateField = channelID + ":" + day
    } else {
      uv = 0L
      dataField = channelID + ":" + day
      aggregateField = channelID + ":" + day
    }
    pvuv.channelID = channelID
    pvuv.userID = userID
    pvuv.pv = pv
    pvuv.uv = uv
    pvuv.dataField = dataField
    pvuv.aggregateField = aggregateField
    out.collect(pvuv)
    /*=================天的PVUV采集完毕=================*/
    if (userState.isMonth) {
      //是这一个月第一次过来
      uv = 1L
      dataField = channelID + ":" + month
      aggregateField = channelID + ":" + month
    } else {
      uv = 0L
      dataField = channelID + ":" + month
      aggregateField = channelID + ":" + month
    }
    pvuv.channelID = channelID
    pvuv.userID = userID
    pvuv.pv = pv
    pvuv.uv = uv
    pvuv.dataField = dataField
    pvuv.aggregateField = aggregateField
    out.collect(pvuv)
    /*=================月的PVUV采集完毕========================*/


  }
}
