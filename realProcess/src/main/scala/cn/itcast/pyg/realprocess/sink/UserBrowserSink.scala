package cn.itcast.pyg.realprocess.sink

import cn.itcast.pyg.realprocess.bean.UserBrowser
import cn.itcast.pyg.realprocess.tools.HBaseUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.functions.sink.SinkFunction

class UserBrowserSink extends SinkFunction[UserBrowser]{
  override def invoke(browser: UserBrowser): Unit = {

    val tableName = "browser"
    val rowKey = browser.dataFiled

    //定义count和新老用户数量的列名
    val countColumn = "count"
    val newCountColumn = "newCount"
    val oldCountColumn = "oldCount"

    var count = browser.count
    var newCount = browser.newCount
    var oldCount = browser.oldCount


    val countData: String = HBaseUtils.getData(tableName, rowKey, countColumn)
    val newCountData: String = HBaseUtils.getData(tableName, rowKey, newCountColumn)
    val oldCountData: String = HBaseUtils.getData(tableName, rowKey, oldCountColumn)

    //如果数据不为空,将本次过来的数据和数据库里面的数据进行累加操作

    if (StringUtils.isNotBlank(countData)) {
      count = count + countData.toLong
    }

    if (StringUtils.isNotBlank(newCountData)) {
      newCount = newCount + newCountData.toLong
    }

    if (StringUtils.isNotBlank(oldCountData)) {
      oldCount = oldCount + oldCountData.toLong
    }

    var map = Map[String, String]()

    map += (countColumn -> count.toString)
    map += (newCountColumn -> newCount.toString)
    map += (oldCountColumn -> oldCount.toString)

    HBaseUtils.putMapData(tableName, rowKey, map)

  }
}
