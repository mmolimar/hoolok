package com.github.mmolimar.hoolok.datasource

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

class HoolokMockRelation(
                          numRows: Int,
                          mockSchema: StructType,
                          locale: Option[String],
                          fakers: Map[String, FakerMethod]
                        )(@transient val sqlContext: SQLContext)
  extends BaseRelation
    with TableScan
    with Serializable {

  val generator: DatasetMockGenerator = DatasetMockGenerator(locale)

  override def schema: StructType = mockSchema

  override def buildScan(): RDD[Row] = {
    generator.createFakeDataframe(sqlContext, numRows, schema, fakers).rdd
  }

}
