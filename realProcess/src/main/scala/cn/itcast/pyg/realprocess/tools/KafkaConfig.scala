package cn.itcast.pyg.realprocess.tools

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}

object KafkaConfig {

//使用Scala的ConfigFactory加载配置文件
    val config: Config = ConfigFactory.load()

  val KAFKA_TOPIC = config.getString("input.topic")

  def getProperties: Properties = {
    var properties = new Properties()

    properties.setProperty("bootstrap.servers", config.getString("bootstrap.servers"))
    properties.setProperty("zookeeper.connect", config.getString("zookeeper.connect"))
    properties.setProperty("input.topic", config.getString("input.topic"))
    properties.setProperty("gruop.id", config.getString("gruop.id"))
    properties.setProperty("enable.auto.commit", config.getString("enable.auto.commit"))
    properties.setProperty("auto.commit.interval.ms", config.getString("auto.commit.interval.ms"))
    properties.setProperty("auto.offset.reset", config.getString("auto.offset.reset"))
    properties.setProperty("key.serializer", config.getString("key.serializer"))
    properties.setProperty("key.deserializer", config.getString("key.deserializer"))
    properties
  }
}