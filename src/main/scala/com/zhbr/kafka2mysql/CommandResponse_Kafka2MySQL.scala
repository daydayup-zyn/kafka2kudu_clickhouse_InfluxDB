package com.zhbr.kafka2mysql

import java.io.{BufferedOutputStream, BufferedReader, ByteArrayOutputStream, FileReader, IOException, ObjectOutputStream}
import java.sql.Timestamp
import java.util
import java.util.{Date, Properties, UUID}
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.zhbr.kafka2mysql.Datas_Kafka2MySQL.{convert, getTargetTableAndMonitoringPointCode, putData2RDBMS, queryRunner, toTimeStamp}
import com.zhbr.util.JDBCUtil

import javax.sql.rowset.serial.SerialBlob
import org.apache.commons.dbutils.QueryRunner
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

import java.text.SimpleDateFormat

object CommandResponse_Kafka2MySQL {

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
  private val kafka_topic = properties.getProperty("kafka.topic.commandResponse")
  private var queryRunner :QueryRunner = null

  def main(args: Array[String]): Unit = {

    //设置日志级别
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    //构建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("CommandResponse_Kafka2MySQL").setMaster("local[6]")

    //构建StreamingContext对象
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(5))
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
    realData.foreachRDD(
      rdd =>if (!rdd.isEmpty()) rdd.foreach(line=>{
        //解析json
        val datas_JSONObject: JSONObject = JSON.parseObject(line).getJSONObject("topicData")
        val mid: Int = datas_JSONObject.getInteger("mid")
        val errcode: Int = datas_JSONObject.getInteger("errcode")
        val dataArray: JSONArray = datas_JSONObject.getJSONArray("body")
        //根据命令id获取融合终端id和命令类型
        val tuple: (String, String) = getCommandRecord(queryRunner,mid)
        val gatewayId = tuple._1
        val cmdType = tuple._2

        //errcode为0时进一步解析json
        if (errcode==0 & dataArray!=null & !dataArray.isEmpty){
          cmdType.toLowerCase match {
              //获取数据command
            case "data_get" => {
              for (x <- 0 to dataArray.size()-1){
                val realDataJson = dataArray.getJSONObject(x)
                val keySet: util.Iterator[String] = realDataJson.keySet().iterator()
                while (keySet.hasNext){
                  val param_key = keySet.next()
                  val param_value = realDataJson.get(param_key)
                  val targetTableAndMonitoringPointCode = getTargetTableAndMonitoringPointCode(queryRunner,gatewayId,param_key)
                  val targetTable = targetTableAndMonitoringPointCode._1
                  val monitoringPointCode = targetTableAndMonitoringPointCode._2
                  //获取当前时间
                  val date: Date = new Date()
                  val sdformat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                  val currentTime: String = sdformat.format(date)
                  //数据保存
                  if (targetTable!=null & monitoringPointCode!=null){
                    putData2RDBMS(queryRunner,targetTable,monitoringPointCode,currentTime,param_value)
                  }
                }
              }
            }
              //设置参数command
            case "parameter_set" => {
              for (x <- 0 to dataArray.size()-1){
                val realDataJson = dataArray.getJSONObject(x)
                val code = realDataJson.getInteger("errcode")
                if (code == 0 & realDataJson.keySet().contains("deviceId")){
                  val deviceName = realDataJson.getString("deviceId")
                  val sampling_interval = realDataJson.getString("sampling_interval")
                  val deviceId = getDeviceId(queryRunner,gatewayId, deviceName)
                  queryRunner.update("update US_APP.SENSOR set ACQUISITIONFREQUENCY = "+sampling_interval+" where SENSORCODE = "+deviceId)
                }else if(code == 0 & realDataJson.keySet().contains("monitorId")){
                  val monitorId = realDataJson.getString("monitorId")
                  val upload_interval = realDataJson.getString("upload_interval")
                  val monitoringPointCode: String = getMonitorId(queryRunner,gatewayId,monitorId)
                  queryRunner.update("update US_APP.MONITORINGPOINTSENSORCONFIGURA set UPLOADFREQUENCY = "+upload_interval+" where MONITORINGPOINTCODE = '"+monitoringPointCode+"'")
                }
              }
            }
              //查询参数（此处不做操作，直接从数据库查询即可）
            case "parameter_get" => {

            }
          }
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
    * 根据命令id获取融合终端id和命令类型
    * @param mid
    */
  private def getCommandRecord(queryRunner :QueryRunner,mid:Int) ={
    var GATEWAYID :String = null
    var CMDTYPE :String = null
    val list: util.List[util.Map[String, AnyRef]] = queryRunner.query("SELECT GATEWAYID,CMDTYPE FROM US_APP.COMMANDRECORD WHERE MID = "+mid,new MapListHandler())
    for (x <- 0 to list.size()-1){
      GATEWAYID = list.get(x).get("GATEWAYID").toString
      CMDTYPE = list.get(x).get("CMDTYPE").toString
    }
    (GATEWAYID,CMDTYPE)
  }

  /**
    * 获取目标表和监测点ID
    * @param deviceId
    * @param param_key
    * @return
    */
  private def getTargetTableAndMonitoringPointCode(queryRunner :QueryRunner,deviceId:String,param_key:String) ={
    var MONITORINGPOINTCODE :String = null
    var TARGETTABLE :String = null
    val maps: util.List[util.Map[String, AnyRef]] = queryRunner.query("SELECT MONITORINGPOINTCODE,TARGETTABLE FROM US_APP.OBJECTMONITORINGPOINTS WHERE PLATFORMCODE = '"+deviceId+"' AND MONITORINGPOINTNAME = '"+param_key+"'",new MapListHandler())
    for (x <- 0 to maps.size()-1){
      MONITORINGPOINTCODE = maps.get(x).get("MONITORINGPOINTCODE").toString
      TARGETTABLE = maps.get(x).get("TARGETTABLE").toString
    }
    (TARGETTABLE,MONITORINGPOINTCODE)
  }

  /**
    * 向数据库插入数据
    * @param targetTable
    * @param pointCode
    * @param eventTime
    * @param param_value
    */
  private def putData2RDBMS(queryRunner :QueryRunner,targetTable:String,pointCode:String,eventTime:String,param_value:Object) ={
    var sqlStr:String = null
    var flag: Int = 0
    val timestamp: Timestamp = toTimeStamp(eventTime)
    val RTDCode = UUID.randomUUID().toString().replace("-","")
    val arrayList = new util.ArrayList[Object]()
    targetTable.toUpperCase match {
      case "RTDINTEM" | "RTDPICTURE" => {
        sqlStr = "insert into "+targetTable+" values(?,?,?,?,?,?)"
        val serialBlob = new SerialBlob(convert(param_value))
        arrayList.add(RTDCode)
        arrayList.add(pointCode)
        arrayList.add(timestamp)
        arrayList.add(null)
        arrayList.add(null)
        arrayList.add(serialBlob)
        flag = queryRunner.update(sqlStr,arrayList.toArray)
      }
      case "RTDTEV" | "RTDULT" | "RTDVIB" =>{
        sqlStr = "insert into "+targetTable+" values(?,?,?,?,?,?,?,?,?,?)"
        flag = queryRunner.update(sqlStr,RTDCode,pointCode,timestamp,null,null,null,null,null,null,param_value)
      }
      case _ => {
        sqlStr = "insert into "+targetTable+" values(?,?,?,?)"
        flag = queryRunner.update(sqlStr,RTDCode,pointCode,timestamp,param_value)
      }
    }
    flag
  }

  /**
    * 字符串时间转timestamp
    * @param time_str
    * @return
    */
  private def toTimeStamp(time_str:String) ={
    if (time_str!=null & time_str.equals("")) {
      val year: String = time_str.substring(0, 4)
      val month: String = time_str.substring(4, 6)
      val day: String = time_str.substring(6, 8)
      val hh: String = time_str.substring(9, 11)
      val mm: String = time_str.substring(11, 13)
      val ss: String = time_str.substring(13, 15)
      val dateTime: String = year + "-" + month + "-" + day + " " + hh + ":" + mm + ":" + ss
      val timestamp: Timestamp = Timestamp.valueOf(dateTime)
      timestamp
    }else{
      null
    }
  }

  /**
    * 获取监测点编号
    * @param gatewayId
    * @param monitorId
    * @return
    */
  private def getMonitorId(queryRunner :QueryRunner,gatewayId:String,monitorId:String) ={
    val list: util.List[util.Map[String, AnyRef]] = queryRunner.query("SELECT MONITORINGPOINTCODE FROM US_APP.OBJECTMONITORINGPOINTS WHERE PLATFORMCODE = '"+gatewayId+"' AND MONITORINGPOINTNAME = '"+monitorId+"'",new MapListHandler())
    if (!list.isEmpty){
      val map: util.Map[String, AnyRef] = list.get(0)
      val MONITORINGPOINTCODE: String = map.get("MONITORINGPOINTCODE").toString
      MONITORINGPOINTCODE
    }else{
      null
    }
  }

  /**
    * 获取传感器编号
    * @param gatewayId
    * @param monitorId
    * @return
    */
  private def getDeviceId(queryRunner :QueryRunner,gatewayId:String,deviceName:String) ={
    val list: util.List[util.Map[String, AnyRef]] = queryRunner.query("select c.SENSORCODE from US_APP.SENSOR c join (select SENSORCODE from US_APP.MONITORINGPOINTSENSORCONFIGURA a join (SELECT MONITORINGPOINTCODE FROM US_APP.OBJECTMONITORINGPOINTS WHERE PLATFORMCODE = '"+gatewayId+"') b\n\ton a.MONITORINGPOINTCODE = b.MONITORINGPOINTCODE) d on c.SENSORCODE = d.SENSORCODE and SENSORNAME = '"+deviceName+"'",new MapListHandler())
    if (!list.isEmpty){
      val map: util.Map[String, AnyRef] = list.get(0)
      val SENSORCODE: String = map.get("SENSORCODE").toString
      SENSORCODE
    }else{
      null
    }
  }

  /**
   * object类型转byte[]
   *
   * @param obj
   * @return
   * @throws IOException
   */
  @throws[IOException]
  private def convert(obj: Any): Array[Byte] = {
    var os: ObjectOutputStream = null
    val byteStream = new ByteArrayOutputStream(5000)
    os = new ObjectOutputStream(new BufferedOutputStream(byteStream))
    os.flush()
    os.writeObject(obj)
    os.flush()
    val sendBuf: Array[Byte] = byteStream.toByteArray
    os.close()
    sendBuf
  }
}
