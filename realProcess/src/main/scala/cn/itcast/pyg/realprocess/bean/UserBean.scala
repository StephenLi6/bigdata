package cn.itcast.pyg.realprocess.bean

import com.alibaba.fastjson.{JSON, JSONObject}

case class UserBean (
                      var browserType: String,
                      var categoryID: String,
                      var channelID: String,
                      var city: String,
                      var country: String,
                      var entryTime: String,
                      var leaveTime: String,
                      var network: String,
                      var produceID: String,
                      var province: String,
                      var source: String,
                      var userID: String
                    )
object UserBean {
  /**
    * 接收一个json字符串,转换为一个UserBean对象
    * @param json
    * @return
    */
  def toBean(json: String): UserBean = {

    //将json转换为json对象
    val jsonObj: JSONObject = JSON.parseObject(json)

    val browserType: String = jsonObj.getString("browserType")
    val categoryID: String = jsonObj.getString("categoryID")
    val channelID: String = jsonObj.getString("channelID")
    val city: String = jsonObj.getString("city")
    val country: String = jsonObj.getString("country")
    val entryTime: String = jsonObj.getString("entryTime")
    val leaveTime: String = jsonObj.getString("leaveTime")
    val network: String = jsonObj.getString("network")
    val produceID: String = jsonObj.getString("produceID")
    val province: String = jsonObj.getString("province")
    val source: String = jsonObj.getString("source")
    val userID: String = jsonObj.getString("userID")

    UserBean(
      browserType,
      categoryID,
      channelID,
      city,
      country,
      entryTime,
      leaveTime,
      network,
      produceID,
      province,
      source,
      userID
    )
  }
}