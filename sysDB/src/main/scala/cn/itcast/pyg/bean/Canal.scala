package cn.itcast.pyg.bean

import scala.collection.mutable.ArrayBuffer

case class Canal (
                 var logfileOffset: Long,
                 var dbName: String,
                 var logfileName: String,
                 var eventType: String,
                 var tableName: String,
                 var rowData: ArrayBuffer[Cell]
                 )
