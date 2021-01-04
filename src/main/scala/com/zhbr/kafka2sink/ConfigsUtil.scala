package com.zhbr.kafka2sink

import com.typesafe.config.{Config, ConfigFactory}

/**
  * 加载application.conf文件 获取配置属性和内容
  */
object ConfigsUtil {

  //创建config对象
  private val config: Config = ConfigFactory.load()

  //获取clickhouse的url
  def getClickhouseUrl = config.getString("clickhouse.url")

  //获取kudu的集群
  def getKuduCluster = config.getString("kudu.cluster")

  //获取kafka集群
  def getKafkaCluster = config.getString("kafka.cluster")

  //获取influxDB的userName
  def getInfluxDBUserName = config.getString("influxDB.username")

  //获取influxDB的password
  def getInfluxDBPassword = config.getString("influxDB.password")

  //获取influxDB的url
  def getInfluxDBUrl = config.getString("influxDB.url")
}
