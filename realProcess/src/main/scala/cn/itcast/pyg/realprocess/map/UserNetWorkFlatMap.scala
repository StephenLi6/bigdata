package cn.itcast.pyg.realprocess.map

import cn.itcast.pyg.realprocess.bean.{Message, UserNetWork}
import cn.itcast.pyg.realprocess.tools.{TimeUtils, UserState, UserStateUtils}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector

class UserNetWorkFlatMap extends FlatMapFunction[Message, UserNetWork]{
  override def flatMap(msg: Message, out: Collector[UserNetWork]): Unit = {
    val work = new UserNetWork

    work.netWorkName = msg.userBean.network
    work.timeStamp = msg.timeStamp

    work.count = 1L

    //获取各个时间段的字符串
    val hour: String = TimeUtils.getTime(work.timeStamp.toString, TimeUtils.HOUR_PATTERN)
    val day: String = TimeUtils.getTime(work.timeStamp.toString, TimeUtils.DAY_PATTERN)
    val month: String = TimeUtils.getTime(work.timeStamp.toString, TimeUtils.MONTH_PATTERN)

//    用户ID + ":" + network

    val rowKey = msg.userBean.userID + ":" + work.netWorkName
    val userState: UserState = UserStateUtils.getUserState(rowKey, work.timeStamp.toString)

    if (userState.isNew) {
      //新用户
      work.newCount = 1L
    } else {
      //老用户
      work.newCount = 0L
    }

    if (userState.isHour) {
      //是这个小时第一次过来
      work.oldCount = 1L
    } else {
      work.oldCount = 0L
    }

//    移动:20190303

    work.dataField = work.netWorkName + ":" + hour
    work.aggregateField = work.netWorkName + ":" + hour

    out.collect(work)


    if (userState.isDay) {
      //是这一天第一次过来
      work.oldCount = 1L
    } else {
      work.oldCount = 0L
    }

    //    移动:20190303

    work.dataField = work.netWorkName + ":" + day
    work.aggregateField = work.netWorkName + ":" + day

    out.collect(work)


    if (userState.isMonth) {
      //是这一月第一次过来
      work.oldCount = 1L
    } else {
      work.oldCount = 0L
    }

    //    移动:20190303

    work.dataField = work.netWorkName + ":" + month
    work.aggregateField = work.netWorkName + ":" + month

    out.collect(work)

  }
}
