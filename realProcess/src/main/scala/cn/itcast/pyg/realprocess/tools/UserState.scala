package cn.itcast.pyg.realprocess.tools

import org.apache.commons.lang3.StringUtils

case class UserState(
                      var isNew: Boolean, //是否是新用户
                      var isHour: Boolean, //是否是这个小时第一次过来
                      var isDay: Boolean, //是否是这一天第一次过来
                      var isMonth: Boolean //是否是这一个月第一次过来
                    )

object UserStateUtils {
  /**
    * 获取用户的状态信息
    *
    * @param userID    用户id
    * @param timeStamp 查询的时间记录
    * @return
    */
  def getUserState(rowKey: String, timeStamp: String): UserState = {
    //初始情况下,默认都为false
    var isNew = false
    var isHour = false
    var isDay = false
    var isMonth = false

    //获取上次访问时间
    val tableName = "userstate"

    val firstColumn = "firstTime" //第一次来的时间
    val lastColumn = "lastTime" //最后一次访问时间
    //查询HBase,只需要查询最后一次访问时间
    val lastTimeData: String = HBaseUtils.getData(tableName, rowKey, lastColumn)
    if (StringUtils.isNotBlank(lastTimeData)) {
      //如果不为空,说明不是第一次来,接下来咋弄?盘他!
      //怎么才能知道,用户是不是这个小时第一次过来?

//      HBase中的历史访问记录:       352452345234545    ->    15:32
//
//      timeStamp:                 352452347634545    ->    16:36   -> 向下取整   "2019032616"  --> 352452343234545

      if (TimeUtils.getTimeStamp(timeStamp, TimeUtils.HOUR_PATTERN) > lastTimeData.toLong) {
        //本小时第一次过来
        isHour = true
      }
      if (TimeUtils.getTimeStamp(timeStamp, TimeUtils.DAY_PATTERN) > lastTimeData.toLong) {
        //本天第一次过来
        isDay = true
      }
      if (TimeUtils.getTimeStamp(timeStamp, TimeUtils.MONTH_PATTERN) > lastTimeData.toLong) {
        //本月第一次过来
        isMonth = true
      }
      //更新用户最后一次访问时间
      HBaseUtils.putData(tableName, rowKey, lastColumn, timeStamp)

    } else {
      //是第一次过来
      isNew = true
      isHour = true
      isDay = true
      isMonth = true
      //将用户访问数据保存起来
      var map = Map[String, String]()
      map += (firstColumn -> timeStamp)
      map += (lastColumn -> timeStamp)

      HBaseUtils.putMapData(tableName, rowKey, map)
    }
    UserState(isNew, isHour, isDay, isMonth)
  }
}
