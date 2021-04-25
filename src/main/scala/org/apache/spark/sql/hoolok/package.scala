package org.apache.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.types.StructType

package object hoolok {

  def createStreamingDataframe(sqlContext: SQLContext, rdd: RDD[Row], schema: StructType): DataFrame = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(schema).asInstanceOf[StructType]
    val catalystRows = rdd.map(RowEncoder(replaced).createSerializer())
    sqlContext.sparkSession.internalCreateDataFrame(catalystRows.setName(rdd.name), schema, isStreaming = true)
  }

}
