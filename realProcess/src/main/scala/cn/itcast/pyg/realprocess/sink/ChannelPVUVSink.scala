package cn.itcast.pyg.realprocess.sink

import cn.itcast.pyg.realprocess.bean.ChannelPVUV
import cn.itcast.pyg.realprocess.tools.HBaseUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.functions.sink.SinkFunction

class ChannelPVUVSink extends SinkFunction[ChannelPVUV]{
  override def invoke(value: ChannelPVUV): Unit = {
    //落地操作
    val tableName = "channel"
    val rowKey = value.dataField
    val pvColumn = "pvNum"
    val uvColumn = "uvNum"

    var pv = value.pv
    var uv = value.uv

    //取HBase里面的数据

    val pvData: String = HBaseUtils.getData(tableName, rowKey, pvColumn)
    val uvData: String = HBaseUtils.getData(tableName, rowKey, uvColumn)

    if (StringUtils.isNotBlank(pvData)) {
      pv = pv + pvData.toLong
    }
    if (StringUtils.isNotBlank(uvData)) {
      uv = uv + uvData.toLong
    }


    //保存最新的数据

    var map = Map[String, String]()

    map += (pvColumn -> pv.toString)
    map += (uvColumn -> uv.toString)

    HBaseUtils.putMapData(tableName, rowKey, map)

  }
}
