package cn.itcast.pyg.realprocess.sink

import cn.itcast.pyg.realprocess.bean.ChannelUserFresh
import cn.itcast.pyg.realprocess.tools.HBaseUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.functions.sink.SinkFunction

class ChannelUserFreshSink extends SinkFunction[ChannelUserFresh]{
  override def invoke(value: ChannelUserFresh): Unit = {
    //数据落地操作

    val tableName = "channel"
    var rowKey = value.dataField
    //列名
    val newColumn = "newCount"
    val oldColumn = "oldCount"

    var newCount = value.newCount
    var oldCount = value.oldCount

    //取原来的老数据
    //HBase中的新增用户数量
    val newData: String = HBaseUtils.getData(tableName, rowKey, newColumn)
    //获取HBase中老用户的数据
    val oldData: String = HBaseUtils.getData(tableName, rowKey, oldColumn)

    if (StringUtils.isNotBlank(newData)) {
      newCount = newCount + newData.toLong
    }

    if (StringUtils.isNotBlank(oldData)) {
      oldCount = oldCount + oldData.toLong
    }
    //将计算后的数据存入HBase
    var map = Map[String, String]()

    map += (newColumn -> newCount.toString)
    map += (oldColumn -> oldCount.toString)

    HBaseUtils.putMapData(tableName, rowKey, map)
  }

}
