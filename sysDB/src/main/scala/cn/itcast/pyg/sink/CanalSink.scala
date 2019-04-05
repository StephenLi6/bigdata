package cn.itcast.pyg.sink

import cn.itcast.pyg.bean.Canal
import cn.itcast.pyg.tools.HBaseUtils
import org.apache.flink.streaming.api.functions.sink.SinkFunction

class CanalSink extends SinkFunction[Canal]{
  override def invoke(canal: Canal): Unit = {
    //将Canal存入HBase,数据落地
    if ("DELETE".equals(canal.eventType)) {
      //删除数据
      HBaseUtils.deleteData(canal)
    } else {
      //添加
      HBaseUtils.putData(canal)
    }
  }
}
