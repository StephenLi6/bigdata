package com.theshy.datastream

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.windows._
import org.apache.flink.streaming.api.windowing.time.Time


/*
* The Best Or Nothing
* Desinger:TheShy
* Date:2019/3/2720:15
* com.theshy.datastreambigdata
*/
object Demo3WaterMarker {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream: DataStream[String] = env.socketTextStream("node1",9000)

    val productStream: DataStream[Product] = dataStream.map(x => {
      val arr: Array[String] = x.split(",")
      Product(arr(0).toLong, arr(1), arr(2), arr(3).toDouble)
    })
    val watermarkStream: DataStream[Product] = productStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[Product] {
      //默认先定义为0L,
      var currentTimestamp = 0L
      //最大延迟时间
      var maxDelay = 2000L

      var watermark: Watermark = null

      //获取当前水印
      override def getCurrentWatermark: Watermark = {
        watermark = new Watermark(currentTimestamp - maxDelay)
        watermark
      }

      //抽取时间戳
      override def extractTimestamp(product: Product, previousElementTimestamp: Long): Long = {
        var time = product.timeStamp
        currentTimestamp = Math.max(time, currentTimestamp)
        println(time + "|" + watermark.getTimestamp)

        currentTimestamp
      }
    })

    val groupData: KeyedStream[Product, String] = watermarkStream.keyBy(line => line.productID)

    val window: WindowedStream[Product, String, TimeWindow] = groupData.timeWindow(Time.seconds(3))

    window.max("price").print()



    env.execute()

  }
}

case class Product(timeStamp:Long,userId:String,productID:String,price:Double)