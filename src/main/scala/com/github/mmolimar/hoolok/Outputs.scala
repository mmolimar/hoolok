package com.github.mmolimar.hoolok

import com.github.mmolimar.hoolok.Utils.inspectOutputs
import com.github.mmolimar.hoolok.annotations.OutputKind
import com.github.mmolimar.hoolok.implicits.DataframeEnricher
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

trait Output {

  val config: HoolokOutputConfig

  def write(): Unit
}

object Output {

  private val outputs = inspectOutputs

  def apply(config: HoolokOutputConfig)(implicit spark: SparkSession): Output = {
    outputs.get(config.kind.trim.toLowerCase)
      .map(clz => clz.getConstructor(classOf[HoolokOutputConfig], classOf[SparkSession]))
      .map(_.newInstance(config, spark))
      .getOrElse {
        throw new InvalidOutputConfigException(s"Output kind '${config.kind}' is not supported.")
      }
  }

}

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

@OutputKind(kind = "stream")
class OutputStreamProcessor(config: HoolokOutputConfig)
                           (implicit spark: SparkSession) extends BaseOutput(config)(spark) {

  def writeInternal(dataframe: DataFrame): Unit = dataframe
    .writeStream
    .outputMode(config.mode)
    .format(config.format)
    .options(config.options.getOrElse(Map.empty))
    .partitionBy(config.partitionBy.orNull: _*)
    .start()
    .awaitTermination()
}

@OutputKind(kind = "batch")
class OutputBatchProcessor(config: HoolokOutputConfig)
                          (implicit spark: SparkSession) extends BaseOutput(config)(spark) {

  def writeInternal(dataframe: DataFrame): Unit = dataframe
    .write
    .mode(config.mode)
    .format(config.format)
    .partitionBy(config.partitionBy.orNull: _*)
    .options(config.options.getOrElse(Map.empty))
    .save()
}
