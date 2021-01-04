package com.zhbr.kafka2sink

import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


object DmpMain {
  def main(args: Array[String]): Unit = {
     //1、构建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("DmpMain").setMaster("local[6]")

     //2、构建SparkSession对象
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    //3、获取sparkContext对象
    val sc: SparkContext = sparkSession.sparkContext
    sc.setLogLevel("warn")

    //4、构建StreamingContext对象
    val ssc: StreamingContext = new StreamingContext(sc,Seconds(5))
    //设置检查点,通常生产环境当中，为了保证数据不丢失，将数据放到hdfs之上，hdfs的高容错，多副本特征
    ssc.checkpoint("./kafka-chk2")

    //5、获取kuduContext对象
    val kuduContext = new KuduContext(ConfigsUtil.getKuduCluster,sc)

    //6、处理业务逻辑
    //kuduContext.deleteTable("emq20201029")
    ImproveData.process(sparkSession,ssc,kuduContext)

    //7、开始程序
    ssc.start()
    ssc.awaitTermination()
  }
}
