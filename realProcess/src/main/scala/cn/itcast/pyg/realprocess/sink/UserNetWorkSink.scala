package cn.itcast.pyg.realprocess.sink

import cn.itcast.pyg.realprocess.bean.UserNetWork
import cn.itcast.pyg.realprocess.tools.HBaseUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.functions.sink.SinkFunction

class UserNetWorkSink extends SinkFunction[UserNetWork]{
  override def invoke(netWork: UserNetWork): Unit = {
    //数据落地代码
    val tableName = "network"
    val rowKey = netWork.dataField
    val countColumn = "count"
    val newCountColumn = "newCount"
    val oldCountColumn = "oldCount"

    var count = netWork.count
    var newCount = netWork.newCount
    var oldCount = netWork.oldCount

    //从HBase里面获取数据
    val countData: String = HBaseUtils.getData(tableName, rowKey, countColumn)
    val newCountData: String = HBaseUtils.getData(tableName, rowKey, newCountColumn)
    val oldCountData: String = HBaseUtils.getData(tableName, rowKey, oldCountColumn)

    //如果不为空,就进行累加操作
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

    //将计算后的数据,存入HBasse
    HBaseUtils.putMapData(tableName, rowKey, map)

  }
}
