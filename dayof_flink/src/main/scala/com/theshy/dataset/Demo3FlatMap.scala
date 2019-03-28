package com.theshy.dataset

import org.apache.flink.api.scala._

/*
* The Best Or Nothing
* Desinger:TheShy
* Date:2019/3/2620:38
* com.thebigdata
*/
object Demo3FlatMap {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val dataset: DataSet[String] = env.fromElements("B;D;A;C;D;A;C;E;F;M;S;C;E")
    //dataset.flatMap(_.split(";")).print()

    dataset.map(line =>
      for(index <- 0 until line.length -1) yield ((line(index)+":"+line(index +1)),1)
    ).print()

    //env.execute()
  }
}
