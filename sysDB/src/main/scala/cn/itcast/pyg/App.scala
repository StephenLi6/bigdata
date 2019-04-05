package cn.itcast.pyg

import java.lang
import java.util.Properties

import cn.itcast.pyg.bean.{Canal, Cell}
import cn.itcast.pyg.sink.CanalSink
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09

import scala.collection.mutable.ArrayBuffer

object App {

  //接收Kafka数据,存入HBase

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()

    properties.setProperty("bootstrap.servers", "node01:9092,node02:9092,node03:9092")
    properties.setProperty("zookeeper.connect", "node01:2181,node02:2181,node03:2181")
    properties.getProperty("group.id", "canal")


    val consumer = new FlinkKafkaConsumer09[String](
      "canal",
      new SimpleStringSchema(),
      properties
    )

    val source: DataStream[String] = env.addSource(consumer)

    source.print()
//    {"logfileOffset":4792,"dbName":"big_data","rowData":
    //    [{"columnUpdated":true,"columnValue":"13","columnName":"id"},{"columnUpdated":true,"columnValue":"af","columnName":"name"},{"columnUpdated":true,"columnValue":"","columnName":"action"},{"columnUpdated":true,"columnValue":"asd","columnName":"style"},{"columnUpdated":true,"columnValue":"","columnName":"description"},{"columnUpdated":true,"columnValue":"","columnName":"parent_id"},{"columnUpdated":true,"columnValue":"","columnName":"deleted"}],"logfileName":"mysql-bin.000001","eventType":"INSERT","tableName":"menu"}

    source.map{
      line =>
        //将json字符串转换为json对象
        val canalObj: JSONObject = JSON.parseObject(line)
        val logfileOffset: lang.Long = canalObj.getLong("logfileOffset")
        val dbName: String = canalObj.getString("dbName")
        val logfileName: String = canalObj.getString("logfileName")
        val eventType: String = canalObj.getString("eventType")
        val tableName: String = canalObj.getString("tableName")
        //rowData集合转换为string字符串取出来
        val arrayJsonStr: String = canalObj.getString("rowData")
//          [
  //          {"columnUpdated":true,"columnValue":"13","columnName":"id"},
  //          {"columnUpdated":true,"columnValue":"13","columnName":"id"},
  //          {"columnUpdated":true,"columnValue":"13","columnName":"id"}
//          ]
        //通过parseArray方法,将json字符串转换为json数组对象
        val array: JSONArray = JSON.parseArray(arrayJsonStr)
        val rowData = ArrayBuffer[Cell]()
        //遍历json数据
        for (index <- 0 to array.size() - 1) {
//          {"columnUpdated":true,"columnValue":"13","columnName":"id"}  ===>转换为字符串
          val cellJsonStr: String = array.get(index).toString
          //将字符串转换为对象
          val cellObj: JSONObject = JSON.parseObject(cellJsonStr)
          val columnUpdated: lang.Boolean = cellObj.getBoolean("columnUpdated")
          val columnValue = cellObj.getString("columnValue")
          val columnName = cellObj.getString("columnName")

          rowData += Cell(columnUpdated, columnValue, columnName)
        }
        Canal(logfileOffset, dbName, logfileName, eventType, tableName, rowData)
    }.addSink(new CanalSink)

    env.execute()
  }

}
