package cn.itcast.pyg.realprocess.bean

class ChannelRegion {

  var channelID: String = null
  var country: String = null
  var province: String = null
  var city: String = null

  var newCount: Long = 0L
  var oldCount: Long = 0L

  var pv: Long = 0L
  var uv: Long = 0L

  var dataField: String = null
  var aggregateField: String = null

}
