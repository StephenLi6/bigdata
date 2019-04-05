package cn.itcast.pyg.realprocess.tools

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

object TimeUtils {

  def HOUR_PATTERN = "yyyyMMddhh"
  def DAY_PATTERN = "yyyyMMdd"
  def MONTH_PATTERN = "yyyyMM"

  /**
    * 格式化时间
    * @param timeStamp 时间戳
    * @param pattern 匹配规则:yyyyMMddhh
    */
  def getTime(timeStamp: String, pattern: String): String = {
    //获取指定类型的格式化工具
    val format: FastDateFormat = FastDateFormat.getInstance(pattern)
    val date = new Date(timeStamp.toLong)
    val formatDate: String = format.format(date)
//    "2019032617"
    formatDate
  }

  /**
    * 根据指定的时间和格式化规则,将时间转换为时间戳
    * @param time 2019032617
    * @param pattern yyyyMMddhh
    * @return
    */
  def getTimeStamp (timeStamp: String, pattern: String):Long = {
    //获取指定类型的格式化工具
    val format: FastDateFormat = FastDateFormat.getInstance(pattern)

    //先将时间戳转换为"2019032617"的格式
    val time: String = getTime(timeStamp, pattern)

    val date: Date = format.parse(time)
    // "245235345245235"
    date.getTime
  }


}
