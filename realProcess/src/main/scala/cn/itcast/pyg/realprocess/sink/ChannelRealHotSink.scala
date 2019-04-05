package cn.itcast.pyg.realprocess.sink

import cn.itcast.pyg.realprocess.bean.ChannelRealHot
import cn.itcast.pyg.realprocess.tools.HBaseUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.functions.sink.SinkFunction

class ChannelRealHotSink extends SinkFunction[ChannelRealHot]{
//ctrl+O
  //数据落地操作,realHot是进行数据落地的对象
  override def invoke(realHot: ChannelRealHot): Unit = {

    val tableName = "channel"
    val rowKey = realHot.channelID
    val columnName = "realHot"

    var count = realHot.count

    //查看原来的HBase中有没有此channelID的记录

    val countData: String = HBaseUtils.getData(tableName, rowKey, columnName)

    if (StringUtils.isNotBlank(countData)) {
      //说明原来的HBase中存在此channelID的数据
      //进行累加操作
      count = count + countData.toLong
    }

    //最终的结果数据
    HBaseUtils.putData(tableName, rowKey, columnName, count.toString)
  }
}
