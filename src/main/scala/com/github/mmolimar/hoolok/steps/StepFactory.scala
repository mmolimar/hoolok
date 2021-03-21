package com.github.mmolimar.hoolok.steps

import com.github.mmolimar.hoolok.HoolokStepConfig
import com.github.mmolimar.hoolok.common.InvalidStepConfigException
import com.github.mmolimar.hoolok.common.Utils.inspectSteps
import org.apache.spark.sql.SparkSession

trait Step {

  val config: HoolokStepConfig

  def process(): Unit

}

object StepFactory {

  private val steps = inspectSteps

  def apply(config: HoolokStepConfig)(implicit spark: SparkSession): Step = {
    steps.get(config.kind.trim.toLowerCase)
      .map(clz => clz.getConstructor(classOf[HoolokStepConfig], classOf[SparkSession]))
      .map(_.newInstance(config, spark))
      .getOrElse {
        throw new InvalidStepConfigException(s"Step kind '${config.kind}' is not supported.")
      }
  }

}
