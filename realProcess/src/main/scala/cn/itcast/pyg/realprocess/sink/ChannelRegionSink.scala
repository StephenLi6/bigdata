package cn.itcast.pyg.realprocess.sink

import cn.itcast.pyg.realprocess.bean.ChannelRegion
import cn.itcast.pyg.realprocess.tools.HBaseUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.functions.sink.SinkFunction

class ChannelRegionSink extends SinkFunction[ChannelRegion]{
  override def invoke(region: ChannelRegion): Unit = {
    //数据落地相关代码

    val tableName = "region"
    val rowKey = region.dataField

    var newCount = region.newCount
    var oldCount = region.oldCount
    var pv = region.pv
    var uv = region.uv

    //列名
    val newCountColumn = "newCount"
    val oldCountColumn = "oldCount"
    val pvColumn = "pv"
    val uvColumn = "uv"

    //原来Hbase中的新增用户量
    val newCountData: String = HBaseUtils.getData(tableName, rowKey, newCountColumn)
    val oldCountData: String = HBaseUtils.getData(tableName, rowKey, oldCountColumn)
    val pvData: String = HBaseUtils.getData(tableName, rowKey, pvColumn)
    val uvData: String = HBaseUtils.getData(tableName, rowKey, uvColumn)

    if (StringUtils.isNotBlank(newCountData)) {
      newCount = newCount + newCountData.toLong
    }

    if (StringUtils.isNotBlank(oldCountData)) {
      oldCount = oldCount + oldCountData.toLong
    }

    if (StringUtils.isNoneBlank(pvData)) {
      pv = pv + pvData.toLong
    }

    if (StringUtils.isNoneBlank(uvData)) {
      uv = uv + uvData.toLong
    }

    var map = Map[String, String]()

    map += (newCountColumn -> newCount.toString)
    map += (oldCountColumn -> oldCount.toString)
    map += (pvColumn -> pv.toString)
    map += (uvColumn -> uv.toString)

    HBaseUtils.putMapData(tableName, rowKey, map)


  }
}
