package com.github.mmolimar.hoolok

import com.github.mmolimar.hoolok.Utils.inspectSteps
import com.github.mmolimar.hoolok.annotations.StepKind
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

trait Step {

  val config: HoolokStepConfig

  def process(): Unit
}

object Step {

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

abstract class BaseStep(override val config: HoolokStepConfig)
                       (implicit spark: SparkSession) extends Step with Logging {

  def process(): Unit = {
    processInternal()
      .createOrReplaceTempView(config.id)
  }

  protected def processInternal(): DataFrame

}

@StepKind(kind = "sql")
class SqlStep(config: HoolokStepConfig)
             (implicit spark: SparkSession) extends BaseStep(config)(spark) {


  val query: String = config.options.flatMap(_.get("query")).getOrElse {
    throw new InvalidConfigException("SQL step is not configured properly. The option 'query' is expected.")
  }

  def processInternal(): DataFrame = {
    logInfo(s"Query to be executed in step: '$query'.")
    spark.sql(query)
  }

}
