package com.theshy.datastream

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/*
* The Best Or Nothing
* Desinger:TheShy
* Date:2019/3/2719:39
* com.theshy.datastreambigdata
*/
object Demo1HelloStream {
  def main(args: Array[String]): Unit = {
    //val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val datastream: DataStream[String] = env.socketTextStream("node1",9000)
    datastream.map(x=>(x,1)).keyBy(_._1).sum(1).print()

    env.execute()
  }
}
