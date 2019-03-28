package com.theshy.dataset

import org.apache.spark.{SparkConf, SparkContext}

/*
* The Best Or Nothing
* Desinger:TheShy
* Date:2019/3/414:55
* com.theshybigdata
*/
object Pv extends App {
  val conf: SparkConf = new SparkConf().setAppName("PV").setMaster("local[2]")
  val sc: SparkContext = new SparkContext(conf)
  //println(sc.textFile("E:\\BoringThing\\hadoop加密視頻21-25天\\05\\资料\\运营商日志").count())
  sc.textFile("E:\\BoringThing\\hadoop加密視頻21-25天\\05\\资料\\运营商日志").map(x => ("pv", 1)).reduceByKey(_ + _).collect().foreach(println)
  sc.textFile("E:\\BoringThing\\hadoop加密視頻21-25天\\05\\资料\\运营商日志").mapPartitions(
    Iterator => Iterator
  )
  sc.stop()
}
