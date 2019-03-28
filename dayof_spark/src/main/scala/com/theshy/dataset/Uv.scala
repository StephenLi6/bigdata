package com.theshy.dataset

import org.apache.spark.{SparkConf, SparkContext}

/*
* The Best Or Nothing
* Desinger:TheShy
* Date:2019/3/415:47
* com.theshybigdata
*/
object Uv extends App {
  val sparkConf: SparkConf = new SparkConf().setAppName("UV").setMaster("local[2]")
  val sc: SparkContext = new SparkContext(sparkConf)
  println(sc.textFile("E:\\BoringThing\\hadoop加密視頻21-25天\\05\\资料\\运营商日志").map(x => x(0)).distinct().count())
  sc.stop()
}
