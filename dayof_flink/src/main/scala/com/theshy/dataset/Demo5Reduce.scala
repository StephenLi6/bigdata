package com.theshy.dataset

import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

/*
* The Best Or Nothing
* Desinger:TheShy
* Date:2019/3/2621:07
* com.thebigdata
*/
object Demo5Reduce {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val dataSet:DataSet[List[Tuple2[String , Int]]] = env.fromElements(List(("java" , 3),("java" , 1) , ("scala" , 1) , ("java" , 1)))
    //传入flatmap里面的数据为("java" , 1),所以,FlatMap直接返回就行了
    val flatMapData: DataSet[(String, Int)] = dataSet.flatMap(line => line)

    //    flatMapData.groupBy(0).sum(1).print()

    val groupData: GroupedDataSet[(String, Int)] = flatMapData.groupBy(_._1)

    /*
    *
    * (java,2) 老的
    *
    * (java,1) 新的
    *
    * (java,3) 结果  / 老的
    *
    *
    * (java,1) 新的
    *
    *
    *
    * */
    //o是老的数据,n是新的数据.(o._1,老的数量+新的数量)
    //val reduceData: DataSet[(String, Int)] = groupData.reduce((o,n) => (o._1, o._2 + n._2))
    //groupData.reduce((x,y)=>(x._1,x._2+y._2)).print()
    groupData.reduceGroup{ (in, out: Collector[(String,Int)]) =>
      in.toSet foreach (out.collect)
  }.print()
    //reduceData.print()



  }
}
