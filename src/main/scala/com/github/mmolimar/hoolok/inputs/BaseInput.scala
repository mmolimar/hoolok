package com.github.mmolimar.hoolok.inputs

import com.github.mmolimar.hoolok.HoolokInputConfig
import com.github.mmolimar.hoolok.common.Implicits.DataframeEnricher
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class BaseInput(override val config: HoolokInputConfig)
                        (implicit spark: SparkSession) extends Input with Logging {

  override final def read(): Unit = {
    logInfo(s"Reading input ${config.kind} for ID '${config.id}' with format '${config.format}'.")
    val df = readInternal
      .possiblyWithCoalesce(config.coalesce)
      .possiblyWithRepartition(config.repartition)
      .possiblyWithWatermark(config.watermark)

    df.createOrReplaceTempView(config.id)
  }

  protected def readInternal: DataFrame

}
