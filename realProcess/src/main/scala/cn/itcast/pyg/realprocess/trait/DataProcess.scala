package cn.itcast.pyg.realprocess.`trait`

import cn.itcast.pyg.realprocess.bean.Message
import org.apache.flink.streaming.api.scala.DataStream

trait DataProcess {

  def process (source: DataStream[Message])
}
