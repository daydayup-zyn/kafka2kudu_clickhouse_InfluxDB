package com.zhbr.kafka2sink

import java.util

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.influxdb.dto.Point
import org.influxdb.{InfluxDB, InfluxDBFactory}

import scala.util.parsing.json.JSON

/**
  * 对数据进行处理并将数据保存到kudu\clickhouse\influxDB
  */
object ImproveData  extends ProcessData {
  //获取kudumaster地址
  private val kuduMaster: String = ConfigsUtil.getKuduCluster
  //获取kudu表名
  private val tableName: String = "emq20201029"

  //配置influxDB
  private var influxDB:InfluxDB = _
  private var point: Point = _
  private val username = ConfigsUtil.getInfluxDBUserName
  private val password = ConfigsUtil.getInfluxDBPassword
  private val openurl = ConfigsUtil.getInfluxDBUrl
  private val database = "zyn"

  //配置clickhouse
  private val cli_database = "default"
  private val cli_tableName = "emq_stream1"

  /**
    * 处理数据，实现不同的etl操作
    *
    * @param
    */
  override def process(sparkSession: SparkSession,ssc:StreamingContext,kuduContext:KuduContext): Unit = {

    //设置kafkaParams
    val kafkaParams=Map(
      "bootstrap.servers"->ConfigsUtil.getKafkaCluster,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id"->"group1")

    //设置topics
    val topics=Set("emq")

    //获取数据
    val data: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    //获取真正的数据,数据在元组的第二位
    val realData: DStream[String] = data.map(x=>x.value())
    realData.print()

    //数据处理逻辑
    val value: DStream[String] = realData.map(rdd => {
      rdd.replace("msg", "humidity")
    })

    //数据保存到clickhouse
    value.foreachRDD(rdd=>{
      rdd.foreach(lineStr=>{
        import com.alibaba.fastjson.{JSON, JSONObject}
        val jSONObject: JSONObject = JSON.parseObject(lineStr)
        val byq: String = jSONObject.getString("byq")
        val address: String = jSONObject.getString("address")
        val temperature: Integer = jSONObject.getInteger("temperature")
        val msg: Integer = jSONObject.getInteger("humidity")

        DataToClickhouse.exeSql(s"insert into $cli_database.$cli_tableName values('"+byq+"','"+address+"',"+temperature+","+msg+")")
      })
    })

    //数据保存到kudu
//    import sparkSession.implicits._
//
//    val kuduClientBuilder = new KuduClient.KuduClientBuilder(kuduMaster)
//    val kuduClient: KuduClient = kuduClientBuilder.build()
//
//    if(!kuduClient.tableExists(tableName)){
//      val options = new CreateTableOptions
//      val partitionList = new util.ArrayList[String]()
//      partitionList.add("rq") //id
//      options.addHashPartitions(partitionList,6)
//      kuduClient.createTable(tableName,ContantsSchema.emqSchema,options)
//    }
//
//    value.foreachRDD(rdd=>{
//      val newRDD: RDD[BeanClass] = rdd.map(lineStr => {
//        val option: Option[Any] = JSON.parseFull(lineStr)
//        val map: Map[String, Any] = option.get.asInstanceOf[Map[String, Any]]
//        new BeanClass(
//          System.nanoTime().toString,
//          map.get("byq").get.asInstanceOf[String],
//          map.get("address").get.asInstanceOf[String],
//          map.get("temperature").get.asInstanceOf[Double],
//          map.get("humidity").get.asInstanceOf[Double]
//        )
//      })
//      val dataFrame: DataFrame = newRDD.toDF()
//      kuduContext.insertRows(dataFrame,tableName)
//    })

    //保存到influxDB
    value.foreachRDD(rdd=>{
      rdd.foreach(lineStr=>{
        val option: Option[Any] = JSON.parseFull(lineStr)
        val map: Map[String, AnyRef] = option.get.asInstanceOf[Map[String, AnyRef]]

        val tagToString:util.Map[String,Object] = new util.HashMap[String,Object]()
        val fieldToString:util.Map[String,Object] = new util.HashMap[String,Object]()

        for (sundry <- map) {
          sundry match {
            case (k,v) if k=="byq" =>tagToString.put(k,v)
            case (k,v) if k=="address"=>tagToString.put(k,v)
            case (k,v) if k=="temperature"=>fieldToString.put(k,v)
            case (k,v) if k=="humidity"=>fieldToString.put(k,v)
          }
        }

        //一条一条写入
        //InfluxDBConnect.insert("tableTest",tagToString.asInstanceOf[util.HashMap[String,String]],fieldToString)
        influxDB = InfluxDBFactory.connect(openurl, username, password)
        point = Point.measurement("tableTest")
            .tag(tagToString.asInstanceOf[util.HashMap[String,String]])
            .fields(fieldToString)
            .build()

        influxDB.write(database,"",point)
        influxDB.close()
      })
    })
  }
}
