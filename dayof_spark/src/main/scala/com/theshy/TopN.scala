package com.theshy

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
* The Best Or Nothing
* Desinger:TheShy
* Date:2019/3/416:07
* com.theshybigdata
*/
object TopN extends App {
  private val conf: SparkConf = new SparkConf().setAppName("TopN").setMaster("local[2]")
  private val sc: SparkContext = new SparkContext(conf)
  sc.textFile("E:\\\\BoringThing\\\\hadoop加密視頻21-25天\\\\05\\\\资料\\\\运营商日志\\access.log").map(_.split(" ")).filter(_.length>10).map(x=>(x(10),1)).reduceByKey(_+_).sortBy(_._2,false).foreach(println)
  sc.stop()
 /*val sparkConf: SparkConf = new SparkConf().setAppName("TopN").setMaster("local[2]")
  val sc: SparkContext = new SparkContext(sparkConf)
  sc.setLogLevel("WARN")
  //读取数据
  val file: RDD[String] = sc.textFile("E:\\\\BoringThing\\\\hadoop加密視頻21-25天\\\\05\\\\资料\\\\运营商日志\\access.log")
  //将一行数据作为输入,输出(来源URL,1)
  val refUrlAndOne: RDD[(String, Int)] = file.map(_.split(" ")).filter(_.length>10).map(x=>(x(10),1))
  //聚合 排序-->降序
  val result: RDD[(String, Int)] = refUrlAndOne.reduceByKey(_+_).sortBy(_._2,false)
  //通过take取topN，这里是取前5名
  val finalResult: Array[(String, Int)] = result.take(5)
  println(finalResult.toBuffer)*/

  sc.stop()

}
