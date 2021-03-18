package com.github.mmolimar.hoolok

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

object Input extends Logging {

  def apply(config: HoolokInputConfig)(implicit spark: SparkSession): Input = config.stream match {
    case Some(true) => new InputStreamProcessor(config)(spark)
    case _ => new InputBatchProcessor(config)(spark)
  }

}

abstract class Input(config: HoolokInputConfig)
                    (implicit spark: SparkSession) extends Logging {

  def read(): Unit = {
    logInfo(s"Reading input for ID '${config.id}' with format '${config.format}'.")
    val df = readInternal
    df.createOrReplaceTempView(config.id)
  }

  protected def readInternal: DataFrame

}

private class InputStreamProcessor(config: HoolokInputConfig)
                                  (implicit spark: SparkSession) extends Input(config)(spark) {

  def readInternal: DataFrame = spark
    .readStream
    .format(config.format)
    .options(config.options.getOrElse(Map.empty))
    .load()

}

private class InputBatchProcessor(config: HoolokInputConfig)
                                 (implicit spark: SparkSession) extends Input(config)(spark) {

  override def readInternal: DataFrame = spark
    .read
    .format(config.format)
    .options(config.options.getOrElse(Map.empty))
    .load()

}
