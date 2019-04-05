package cn.itcast.pyg.realprocess.bean

class ChannelPVUV {

  var channelID: String = null
  var userID: String = null
  var pv: Long = 0L
  var uv: Long = 0L
  var timeStamp: Long = 0L

  //用于数据存储,rowKey
  var dataField: String = null
  //用于分组的字段
  var aggregateField: String = null

}
