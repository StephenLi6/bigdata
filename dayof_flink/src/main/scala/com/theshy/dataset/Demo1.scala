package com.theshy.dataset

import org.apache.flink.api.scala._

/*
* The Best Or Nothing
* Desinger:TheShy
* Date:2019/3/2620:27
* com.thebigdata
*/
object Demo1 {
  /*def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val dataset: DataSet[String] = env.fromElements("java","scala","flink","scala")
    dataset.map(x => (x,1)).groupBy(0).sum(1).print()
    env.execute()
  }*/
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val dataSet: DataSet[(String, Int)] = env.fromElements(("A" , 1) , ("B" , 1) , ("C" , 1))
    dataSet.map(line => line._1 + "=" + line._2).print()
    println("------------------")
    dataSet.flatMap(line => line._1 + "=" + line._2).print()
  }
}
