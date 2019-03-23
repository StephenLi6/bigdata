package com.theshy

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
* The Best Or Nothing
* Desinger:TheShy
* Date:2019/2/2819:30
* com.theshybigdata
*/
//todo:需求：利用scala语言实现spark wordcount程序
object WordCount {
  def main(args: Array[String]): Unit = {
    //1、创建sparkConf对象,设置appName和master的地址，local[2]表示本地运行2个线程
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[2]")

    //2、创建sparkcontext对象
    val sc = new SparkContext(sparkConf)

    //设置日志输出级别
    sc.setLogLevel("WARN")

    // 3、读取数据文件
    val data: RDD[String] = sc.textFile("C:\\Users\\GoGoing.000\\Desktop\\everydaygoal.txt")

    //4、切分文件中的每一行,返回文件所有单词
    val words: RDD[String] = data.flatMap(_.split(" "))

    //5、每个单词记为1，(单词，1)
    val wordAndOne: RDD[(String, Int)] = words.map((_,1))

    //6、相同单词出现的次数累加
    val result: RDD[(String, Int)] = wordAndOne.reduceByKey(_+_)

    //按照单词出现的次数降序排列
    val sortResult: RDD[(String, Int)] = result.sortBy(_._2,false)

    //7、收集结果数据
    val finalResult: Array[(String, Int)] = sortResult.collect()

    //8、打印结果数据
    finalResult.foreach(x=>println(x))


    //9、关闭sc
    sc.stop()
  }
}
