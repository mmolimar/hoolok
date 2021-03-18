package com.github.mmolimar.hoolok

import com.github.mmolimar.hoolok.implicits.DataframeEnricher
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

object Output extends Logging {

  def apply(config: HoolokOutputConfig)(implicit spark: SparkSession): Output = {
    config.stream match {
      case Some(true) => new OutputStreamProcessor(config)(spark)
      case _ => new OutputBatchProcessor(config)(spark)
    }
  }

}

abstract class Output(config: HoolokOutputConfig)
                     (implicit spark: SparkSession) extends Logging {

  final def write(): Unit = {
    logInfo(s"Writing dataframe for table '${config.id}' with format '${config.format}'.'")
    val dataframe = spark.table(config.id)
      .possiblyWithCoalesce(config.coalesce)
      .possiblyWithRepartition(config.repartition)

    writeInternal(dataframe)
  }

  protected def writeInternal(dataFrame: DataFrame): Unit

}

private class OutputStreamProcessor(config: HoolokOutputConfig)
                                   (implicit spark: SparkSession) extends Output(config)(spark) {

  def writeInternal(dataframe: DataFrame): Unit = dataframe
    .writeStream
    .outputMode(config.mode)
    .format(config.format)
    .options(config.options.getOrElse(Map.empty))
    .partitionBy(config.partitionBy.orNull: _*)
    .start()
    .awaitTermination()
}

private class OutputBatchProcessor(config: HoolokOutputConfig)
                                  (implicit spark: SparkSession) extends Output(config)(spark) {

  def writeInternal(dataframe: DataFrame): Unit = dataframe
    .write
    .mode(config.mode)
    .format(config.format)
    .partitionBy(config.partitionBy.orNull: _*)
    .options(config.options.getOrElse(Map.empty))
    .save()
}
