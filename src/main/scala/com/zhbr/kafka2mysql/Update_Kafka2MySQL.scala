package com.zhbr.kafka2mysql

import java.io.{BufferedReader, FileReader}
import java.util.Properties
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.zhbr.util.JDBCUtil
import org.apache.commons.dbutils.{QueryRunner}
import org.apache.commons.dbutils.handlers.MapListHandler
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.util

object Update_Kafka2MySQL {

  //读取配置文件
  private val p_dir: String = System.getProperty("user.dir")
  private val properties = new Properties
  private val bufferedReader = new BufferedReader(new FileReader(p_dir + "/app.properties"))
  properties.load(bufferedReader)
  private val db_url = properties.getProperty("db.url")
  private val db_driver = properties.getProperty("db.driver")
  private val db_userName = properties.getProperty("db.userName")
  private val db_password = properties.getProperty("db.password")
  private val kafka_servers = properties.getProperty("kafka.servers")
  private val kafka_group = properties.getProperty("kafka.group")
  private val kafka_topic = properties.getProperty("kafka.topic.update")
  private var queryRunner :QueryRunner = null

  def main(args: Array[String]): Unit = {

    //设置日志级别
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    //构建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("Update_Kafka2MySQL").setMaster("local[6]")

    //构建StreamingContext对象
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(1))
    //设置检查点,通常生产环境当中，为了保证数据不丢失，将数据放到hdfs之上，hdfs的高容错，多副本特征
    ssc.checkpoint("./kafka-chk2")

    //设置kafkaParams
    val kafkaParams=Map(
      "bootstrap.servers" -> kafka_servers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> kafka_group)

    //设置topics
    val topics=Set(kafka_topic)

    //获取kafka数据
    val data: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    //获取真正的数据,数据在元组的第二位
    val realData: DStream[String] = data.map(x=>x.value())

    //使用foreachRDD()方法将数据写入外部数据库mysql中
    queryRunner = JDBCUtil.getQueryRunner()
    var status : String = null
    realData.foreachRDD(
      rdd =>if (!rdd.isEmpty()) rdd.foreach(line=>{
        val gatewayId_str = JSON.parseObject(line).getString("gatewayId").substring(12)
        val gatewayId = gatewayId_str.substring(0, gatewayId_str.indexOf("/"))
        val datas_JSONObject: JSONObject = JSON.parseObject(line).getJSONObject("topicData")
        val datas_JSONArray: JSONArray = datas_JSONObject.getJSONArray("deviceStatuses")
        for (x <- 0 to datas_JSONArray.size()-1){
          val realDataJson = datas_JSONArray.getJSONObject(x)
          val deviceName = realDataJson.getString("deviceId")
          if (realDataJson.getString("status").equals("online")){
            status = "在线"
          }else{
            status = "离线"
          }
          val deviceId = getDeviceId(queryRunner, gatewayId, deviceName)
          queryRunner.update("update US_APP.SENSOR set OPERATIONSTATUS = '"+status+"' where SENSORCODE = "+deviceId)
        }
      })
    )
    realData.print()

    //7、开始程序
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 使用sparkSQL将json数据保存到MySQL
    * @param line
    * @param table
    * @param sql
    */
  private def dataFrame2MySQL(sparkconf:SparkConf,line:String,table:String,sql:String): Unit ={
    val session = SparkSession.builder().config(sparkconf).getOrCreate()
    val prop = new Properties()
    prop.setProperty("user", db_userName)
    prop.setProperty("password", db_password)
    prop.setProperty("driver", db_driver)
    prop.setProperty("url",db_url)
    import session.implicits._
    val DF = session.read.json(session.createDataset(Seq(line)))
    DF.createOrReplaceTempView("temp")
    val result = session.sql(sql)
    result.write.mode(SaveMode.Append).jdbc(db_url,table,prop)
  }


  /**
   * 获取传感器编号
   * @param gatewayId
   * @param deviceName
   * @return
   */
  private def getDeviceId(queryRunner :QueryRunner,gatewayId:String,deviceName:String) ={
    val list: util.List[util.Map[String, AnyRef]] = queryRunner.query("select c.SENSORCODE from US_APP.SENSOR c join (select SENSORCODE from US_APP.MONITORINGPOINTSENSORCONFIGURA a join (SELECT MONITORINGPOINTCODE FROM US_APP.OBJECTMONITORINGPOINTS WHERE PLATFORMCODE = '"+gatewayId+"') b\n\ton a.MONITORINGPOINTCODE = b.MONITORINGPOINTCODE) d on c.SENSORCODE = d.SENSORCODE and SENSORNAME = '"+deviceName+"'",new MapListHandler())
    val map: util.Map[String, AnyRef] = list.get(0)
    val SENSORCODE: String = map.get("SENSORCODE").toString
    SENSORCODE
  }
}
