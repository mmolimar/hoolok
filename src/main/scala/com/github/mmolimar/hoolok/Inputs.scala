package com.github.mmolimar.hoolok

import com.github.mmolimar.hoolok.Utils.inspectInputs
import com.github.mmolimar.hoolok.annotations.InputKind
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

trait Input {

  val config: HoolokInputConfig

  def read(): Unit
}

object Input {

  private val inputs = inspectInputs

  def apply(config: HoolokInputConfig)(implicit spark: SparkSession): Input = {
    inputs.get(config.kind.trim.toLowerCase)
      .map(clz => clz.getConstructor(classOf[HoolokInputConfig], classOf[SparkSession]))
      .map(_.newInstance(config, spark))
      .getOrElse {
        throw new InvalidInputConfigException(s"Input kind '${config.kind}' is not supported.")
      }
  }

}

abstract class BaseInput(override val config: HoolokInputConfig)
                        (implicit spark: SparkSession) extends Input with Logging {

  def read(): Unit = {
    logInfo(s"Reading input for ID '${config.id}' with format '${config.format}'.")
    val df = readInternal
    df.createOrReplaceTempView(config.id)
  }

  protected def readInternal: DataFrame

}

@InputKind(kind = "stream")
private class StreamBasedInput(config: HoolokInputConfig)
                              (implicit spark: SparkSession) extends BaseInput(config)(spark) {

  def readInternal: DataFrame = spark
    .readStream
    .format(config.format)
    .options(config.options.getOrElse(Map.empty))
    .load()

}

@InputKind(kind = "batch")
private class BatchBasedInput(config: HoolokInputConfig)
                             (implicit spark: SparkSession) extends BaseInput(config)(spark) {

  override def readInternal: DataFrame = spark
    .read
    .format(config.format)
    .options(config.options.getOrElse(Map.empty))
    .load()

}
