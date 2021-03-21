package com.github.mmolimar.hoolok.inputs

import com.github.mmolimar.hoolok.HoolokInputConfig
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class BaseInput(override val config: HoolokInputConfig)
                        (implicit spark: SparkSession) extends Input with Logging {

  def read(): Unit = {
    logInfo(s"Reading input for ID '${config.id}' with format '${config.format}'.")
    val df = readInternal
    df.createOrReplaceTempView(config.id)
  }

  protected def readInternal: DataFrame

}
