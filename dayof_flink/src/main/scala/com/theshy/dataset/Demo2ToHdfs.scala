package com.theshy.dataset

import org.apache.flink.api.scala._

/*
* The Best Or Nothing
* Desinger:TheShy
* Date:2019/3/2620:34
* com.thebigdata
*/
object Demo2ToHdfs {
  def main(args: Array[String]): Unit = {
    //来一个执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //设置并行度为1,为了输出到wordcount文件中
    env.setParallelism(1)
    //加载数据
    val dataSet: DataSet[String] = env.fromElements("scala", "java", "scala", "flink")

    val mapData: DataSet[(String, Int)] = dataSet.map(line => (line, 1))

    val groupData: GroupedDataSet[(String, Int)] = mapData.groupBy(0)

    val sumData: AggregateDataSet[(String, Int)] = groupData.sum(1)


    sumData.writeAsText("hdfs://node1:8020/Flinkword")

    env.execute()


  }
}
