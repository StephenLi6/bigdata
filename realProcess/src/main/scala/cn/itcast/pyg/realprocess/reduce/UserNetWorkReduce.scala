package cn.itcast.pyg.realprocess.reduce

import cn.itcast.pyg.realprocess.bean.UserNetWork
import org.apache.flink.api.common.functions.ReduceFunction

class UserNetWorkReduce extends ReduceFunction[UserNetWork]{
  override def reduce(oldData: UserNetWork, newData: UserNetWork): UserNetWork = {
    val work = new UserNetWork
    work.netWorkName = oldData.netWorkName
    work.timeStamp = oldData.timeStamp
    work.count = oldData.count + newData.count
    work.newCount = oldData.newCount + newData.newCount
    work.oldCount = oldData.oldCount + newData.oldCount

    work.dataField = oldData.dataField
    work.aggregateField = oldData.aggregateField

    work
  }
}
