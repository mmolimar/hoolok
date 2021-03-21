package com.github.mmolimar.hoolok.inputs

import com.github.mmolimar.hoolok.HoolokInputConfig
import com.github.mmolimar.hoolok.common.InvalidInputConfigException
import com.github.mmolimar.hoolok.common.Utils.inspectInputs
import org.apache.spark.sql.SparkSession


trait Input {

  val config: HoolokInputConfig

  def read(): Unit
}

object InputFactory {

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
