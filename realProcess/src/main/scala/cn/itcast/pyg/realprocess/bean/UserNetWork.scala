package cn.itcast.pyg.realprocess.bean

class UserNetWork {

  var netWorkName: String = null
  var timeStamp: Long = 0L
  var count: Long = 0L //使用该网络的总人数
  var newCount: Long = 0L
  var oldCount: Long = 0L

  var dataField: String = null    //rowKey
  var aggregateField: String = null  //分组字段

}
