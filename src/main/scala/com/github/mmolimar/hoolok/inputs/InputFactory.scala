package com.github.mmolimar.hoolok.inputs

import com.github.mmolimar.hoolok.HoolokInputConfig
import com.github.mmolimar.hoolok.common.InvalidInputConfigException
import com.github.mmolimar.hoolok.common.Utils.{inspectBatchInputs, inspectStreamInputs}
import org.apache.spark.sql.SparkSession


trait Input {

  val config: HoolokInputConfig

  def read(): Unit
}

object InputFactory {

  private val batchInputs = inspectBatchInputs

  private val streamInputs = inspectStreamInputs

  def apply(config: HoolokInputConfig)(implicit spark: SparkSession): Input = {
    val inputs = config.kind.trim.toLowerCase match {
      case "batch" => batchInputs
      case "stream" => streamInputs
      case _ => throw new InvalidInputConfigException(s"Input kind '${config.kind}' is not supported.")
    }

    inputs.get(config.subtype)
      .map(clz => clz.getConstructor(classOf[HoolokInputConfig], classOf[SparkSession]))
      .map(_.newInstance(config, spark))
      .getOrElse {
        throw new InvalidInputConfigException(s"Input kind '${config.kind}' with subtype " +
          s"'${config.subtype}' is not supported.")
      }
  }

}
