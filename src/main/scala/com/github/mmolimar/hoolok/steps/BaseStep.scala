package com.github.mmolimar.hoolok.steps

import com.github.mmolimar.hoolok._
import com.github.mmolimar.hoolok.dq.DeequValidation
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class BaseStep(override val config: HoolokStepConfig)
                       (implicit spark: SparkSession) extends Step with Logging {

  override final def process(): Unit = {
    logInfo(s"Processing step '${config.kind}' for ID '${config.id}'.")
    val dataframe = processInternal()
    config.dq.foreach(dq =>
      if (dataframe.isStreaming) {
        logWarning(s"Data quality '${config.kind}' for ID '${config.id}' cannot be executed inside a step " +
          "due to it is a streaming source.")
      } else {
        logInfo(s"Validating data quality in step '${config.kind}' for ID '${config.id}'.")
        new DeequValidation(dq).applyDataQuality(config.kind, config.id, dataframe)
      }
    )
    dataframe.createOrReplaceTempView(config.id)
  }

  protected def processInternal(): DataFrame

}

abstract class DataframeBatchBasedStep(override val config: HoolokStepConfig)
                                      (implicit spark: SparkSession) extends BaseStep(config) {

  override protected def processInternal(): DataFrame = {
    val dataframe = spark.table(config.id)
    if (dataframe.isStreaming) {
      logWarning(s"Step '${config.kind}' cannot be executed due to it is a streaming source for ID '${config.id}'.")
      dataframe
    } else {
      fromDataframe(dataframe)
    }
  }

  protected def fromDataframe(dataframe: DataFrame): DataFrame

}
