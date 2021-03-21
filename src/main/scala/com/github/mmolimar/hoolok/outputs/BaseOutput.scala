package com.github.mmolimar.hoolok.outputs

import com.github.mmolimar.hoolok.HoolokOutputConfig
import org.apache.spark.internal.Logging
import com.github.mmolimar.hoolok.implicits.DataframeEnricher
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class BaseOutput(override val config: HoolokOutputConfig)
                         (implicit spark: SparkSession) extends Output with Logging {

  final def write(): Unit = {
    logInfo(s"Writing dataframe for table '${config.id}' with format '${config.format}'.'")
    val dataframe = spark.table(config.id)
      .possiblyWithCoalesce(config.coalesce)
      .possiblyWithRepartition(config.repartition)

    writeInternal(dataframe)
  }

  protected def writeInternal(dataFrame: DataFrame): Unit

}
