package cn.itcast.pyg.realprocess.bean

case class Message (
                   var timeStamp: Long,//时间戳
                   var count: Int,//当前消息的数量
                   var userBean: UserBean//消息内容
                   )
