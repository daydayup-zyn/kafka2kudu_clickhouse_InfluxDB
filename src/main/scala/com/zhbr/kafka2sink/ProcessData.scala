package com.zhbr.kafka2sink

import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext


//todo:处理数据的接口
trait ProcessData {

  /**
    * 处理数据，实现不同的etl操作
    * @param
    */
  def process(sparkSession: SparkSession,ssc:StreamingContext,kuduContext:KuduContext)

}
