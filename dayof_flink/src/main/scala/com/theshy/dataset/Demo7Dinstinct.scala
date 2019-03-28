package com.theshy.dataset

import org.apache.flink.api.scala._

import scala.collection.mutable
import scala.util.Random

/*
* The Best Or Nothing
* Desinger:TheShy
* Date:2019/3/2719:28
* com.theshybigdata
*/
object Demo7Dinstinct {
  def main(args: Array[String]): Unit = {
    val data: mutable.MutableList[(Int, String, Double)] = new mutable.MutableList[(Int,String,Double)]
    data.+=((1,"yuwen",90.0))
    data.+=((2, "shuxue", 20.0))
    data.+=((3, "yingyu", 30.0))
    data.+=((4, "wuli", 40.0))
    data.+=((5, "yuwen", 50.0))
    data.+=((6, "wuli", 60.0))
    data.+=((7, "yuwen", 70.0))
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val input: DataSet[(Int, String, Double)] = env.fromCollection(Random.shuffle(data))
    input.distinct(1).print()
  }
}
