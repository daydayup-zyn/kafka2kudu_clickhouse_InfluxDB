package com.zhbr.kafka2sink

import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.{Schema, Type}

import scala.collection.JavaConverters._


object ContantsSchema {

  lazy val emqSchema: Schema = {
    val columns = List(
      new ColumnSchemaBuilder("rq", Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("byq", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("address", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("temperature", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("humidity", Type.DOUBLE).nullable(false).build()
    ).asJava
    new Schema(columns)
  }

}
