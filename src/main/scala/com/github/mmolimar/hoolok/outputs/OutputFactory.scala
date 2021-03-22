package com.github.mmolimar.hoolok.outputs

import com.github.mmolimar.hoolok.HoolokOutputConfig
import com.github.mmolimar.hoolok.common.InvalidOutputConfigException
import com.github.mmolimar.hoolok.common.Utils.{inspectBatchOutputs, inspectStreamOutputs}
import org.apache.spark.sql.SparkSession

trait Output {

  val config: HoolokOutputConfig

  def write(): Unit

}

object OutputFactory {

  private val batchOutputs = inspectBatchOutputs

  private val streamOutputs = inspectStreamOutputs

  def apply(config: HoolokOutputConfig)(implicit spark: SparkSession): Output = {
    val outputs = config.kind.trim.toLowerCase match {
      case "batch" => batchOutputs
      case "stream" => streamOutputs
      case _ => throw new InvalidOutputConfigException(s"Output kind '${config.kind}' is not supported.")
    }
    outputs.get(config.subtype)
      .map(clz => clz.getConstructor(classOf[HoolokOutputConfig], classOf[SparkSession]))
      .map(_.newInstance(config, spark))
      .getOrElse {
        throw new InvalidOutputConfigException(s"Output kind '${config.kind}' with subtype " +
          s"'${config.subtype}' is not supported.")
      }
  }

}
