package com.theshy.dataset

import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala._

import scala.collection.mutable
import scala.util.Random

/*
* The Best Or Nothing
* Desinger:TheShy
* Date:2019/3/2621:14
* com.thebigdata
*/
object Demo6Join {
  def main(args: Array[String]): Unit = {
    val data1: mutable.MutableList[(Int, String, Double)] = new mutable.MutableList[(Int,String,Double)]
    data1.+=((1,"yuwen",90.0))
    data1.+=((2, "shuxue", 20.0))
    data1.+=((3, "yingyu", 30.0))
    data1.+=((4, "yuwen", 40.0))
    data1.+=((5, "shuxue", 50.0))
    data1.+=((6, "yingyu", 60.0))
    data1.+=((7, "yuwen", 70.0))
    data1.+=((8, "yuwen", 20.0))
    val data2 = new mutable.MutableList[(Int, String)]
    //学号 ---班级
    data2.+=((1,"class_1"))
    data2.+=((2,"class_1"))
    data2.+=((3,"class_2"))
    data2.+=((4,"class_2"))
    data2.+=((5,"class_3"))
    data2.+=((6,"class_3"))
    data2.+=((7,"class_4"))
    data2.+=((8,"class_1"))

    val env:ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val input1: DataSet[(Int, String, Double)] = env.fromCollection(Random.shuffle(data1))
    val input2: DataSet[(Int, String)] = env.fromCollection(Random.shuffle(data2))
    val data: DataSet[(Int, String, String, Double)] = input2.join(input1).where(0).equalTo(0) {
      (input2, input1) => (input2._1, input2._2, input1._2, input1._3)
    }
    data.groupBy(1).aggregate(Aggregations.MAX,3).print()
//    data.groupBy(1).


  }
}
