package cn.itcast.pyg.realprocess.reduce

import cn.itcast.pyg.realprocess.bean.UserBrowser
import org.apache.flink.api.common.functions.ReduceFunction

class UserBrowserReduce extends ReduceFunction[UserBrowser]{
  override def reduce(oldData: UserBrowser, newData: UserBrowser): UserBrowser = {

    val browser = new UserBrowser
    browser.browserName = oldData.browserName
    browser.timeStamp = oldData.timeStamp

    browser.count = oldData.count + newData.count
    browser.newCount = oldData.newCount + newData.newCount
    browser.oldCount = oldData.oldCount + newData.oldCount

    browser.dataFiled = oldData.dataFiled
    browser.aggregateField = oldData.aggregateField

    browser
  }
}
