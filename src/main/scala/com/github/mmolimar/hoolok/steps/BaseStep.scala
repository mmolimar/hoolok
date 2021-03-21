package com.github.mmolimar.hoolok.steps

import com.github.mmolimar.hoolok.HoolokStepConfig
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class BaseStep(override val config: HoolokStepConfig)
                       (implicit spark: SparkSession) extends Step with Logging {

  def process(): Unit = {
    processInternal()
      .createOrReplaceTempView(config.id)
  }

  protected def processInternal(): DataFrame

}
