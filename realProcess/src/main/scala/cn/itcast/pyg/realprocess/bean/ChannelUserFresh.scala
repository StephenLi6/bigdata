package cn.itcast.pyg.realprocess.bean

class ChannelUserFresh {

  var channelID: String = null
  var userID: String = null
  var timeStamp: Long = 0L
  var newCount: Long = 0L  //新用户数量
  var oldCount: Long = 0L  //老用户数量

  var dataField: String = null
  var aggregateField: String = null

}
