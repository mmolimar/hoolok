package com.github.mmolimar.hoolok.datasource

import org.apache.spark.sql.execution.streaming.{LongOffset, Offset, Source}
import org.apache.spark.sql.hoolok._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}

class HoolokMockSource(
                        numRows: Int,
                        mockSchema: StructType,
                        locale: Option[String],
                        fakers: Map[String, FakerMethod]
                      )(@transient val sqlContext: SQLContext)
  extends Source {

  private var recordOffset = LongOffset(0L)

  val generator: DatasetMockGenerator = DatasetMockGenerator(locale)

  override def schema: StructType = mockSchema

  override def getOffset: Option[Offset] = Some(recordOffset)

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    recordOffset = recordOffset + 1
    createStreamingDataframe(sqlContext, generator.createFakeDataframe(sqlContext, numRows, schema, fakers).rdd, schema)
  }

  override def stop(): Unit = ()

  override def toString: String = s"${getClass.getName}(numRows=$numRows)"

}
