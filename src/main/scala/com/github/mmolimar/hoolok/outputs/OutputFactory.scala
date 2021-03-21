package com.github.mmolimar.hoolok.outputs

import com.github.mmolimar.hoolok.HoolokOutputConfig
import com.github.mmolimar.hoolok.common.InvalidOutputConfigException
import com.github.mmolimar.hoolok.common.Utils.inspectOutputs
import org.apache.spark.sql.SparkSession

trait Output {

  val config: HoolokOutputConfig

  def write(): Unit

}

object OutputFactory {

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
