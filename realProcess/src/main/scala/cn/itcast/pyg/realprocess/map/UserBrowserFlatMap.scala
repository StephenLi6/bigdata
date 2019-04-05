package cn.itcast.pyg.realprocess.map

import cn.itcast.pyg.realprocess.bean.{Message, UserBrowser}
import cn.itcast.pyg.realprocess.tools.{TimeUtils, UserState, UserStateUtils}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector

class UserBrowserFlatMap extends FlatMapFunction[Message, UserBrowser]{
  override def flatMap(msg: Message, out: Collector[UserBrowser]): Unit = {

    val browser = new UserBrowser
    browser.browserName = msg.userBean.browserType
    browser.timeStamp = msg.timeStamp
    browser.count = 1L//来一个用户,就认为使用了一次

    val hour: String = TimeUtils.getTime(browser.timeStamp.toString, TimeUtils.HOUR_PATTERN)
    val day: String = TimeUtils.getTime(browser.timeStamp.toString, TimeUtils.DAY_PATTERN)
    val month: String = TimeUtils.getTime(browser.timeStamp.toString, TimeUtils.MONTH_PATTERN)


    val userStateRowKey = msg.userBean.userID + ":" + browser.browserName
    val userState: UserState = UserStateUtils.getUserState(userStateRowKey, browser.timeStamp.toString)

    if (userState.isNew) {
      //是新用户
      browser.newCount = 1L
    } else {
      browser.newCount = 0L
    }

    //是这个小时第一次过来
    if (userState.isHour) {
      browser.oldCount = 1L
    } else {
      browser.oldCount = 0L
    }

    browser.dataFiled = browser.browserName + ":" + hour
    browser.aggregateField = browser.browserName + ":" + hour

    out.collect(browser)


    //是这一天第一次过来
    if (userState.isHour) {
      browser.oldCount = 1L
    } else {
      browser.oldCount = 0L
    }

    browser.dataFiled = browser.browserName + ":" + day
    browser.aggregateField = browser.browserName + ":" + day

    out.collect(browser)

    //是这一个月第一次过来
    if (userState.isHour) {
      browser.oldCount = 1L
    } else {
      browser.oldCount = 0L
    }

    browser.dataFiled = browser.browserName + ":" + month
    browser.aggregateField = browser.browserName + ":" + month

    out.collect(browser)


  }
}
