package com.theshy.datastream

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/*
* The Best Or Nothing
* Desinger:TheShy
* Date:2019/3/2719:44
* com.theshy.datastreambigdata
*/
object Demo2Road {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream: DataStream[String] = env.socketTextStream("node1",9000)
    dataStream.map(
      x=>{
        val arr: Array[String] = x.split(",")
        Road(arr(0),arr(1).toInt)
      }
    ).keyBy(line => line.loadID).timeWindow(Time.seconds(5))
      .reduce((oldRoad:Road,newRoad:Road)=> Road(oldRoad.loadID,oldRoad.carNum+newRoad.carNum)).print()


    env.execute()
  }
}

case class Road(
  loadID:String,
  carNum:Int
)