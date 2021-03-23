package com.github.mmolimar.hoolok.steps

import com.github.mmolimar.hoolok.HoolokStepConfig
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class BaseStep(override val config: HoolokStepConfig)
                       (implicit spark: SparkSession) extends Step with Logging {

  def process(): Unit = processInternal().createOrReplaceTempView(config.id)

  protected def processInternal(): DataFrame

}

abstract class DataframeBasedStep(override val config: HoolokStepConfig)
                                 (implicit spark: SparkSession) extends BaseStep(config) {

  override protected def processInternal(): DataFrame = fromDataframe(spark.table(config.id))

  protected def fromDataframe(dataframe: DataFrame): DataFrame

}

abstract class DataframeBatchBasedStep(override val config: HoolokStepConfig)
                                      (implicit spark: SparkSession) extends BaseStep(config) {

  override protected def processInternal(): DataFrame = {
    val dataframe = spark.table(config.id)
    if (dataframe.isStreaming) {
      logWarning(s"Step '${config.kind}' cannot be executed due to it is a streaming source for ID '${config.id}'")
      dataframe
    } else {
      fromDataframe(spark.table(config.id))
    }
  }

  protected def fromDataframe(dataframe: DataFrame): DataFrame

}
