package com.github.mmolimar.hoolok

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

object Step extends Logging {

  def apply(config: HoolokStepConfig)(implicit spark: SparkSession): Step = {
    config.`type`.trim.toLowerCase match {
      case "sql" => new SqlStep(config)(spark)
      case _ => throw new InvalidStepConfigException(s"Step type '${config.`type`}' is not supported.")
    }
  }

}

abstract class Step(config: HoolokStepConfig)
                   (implicit spark: SparkSession) extends Logging {

  def process(): Unit = {
    processInternal()
      .createOrReplaceTempView(config.id)
  }

  protected def processInternal(): DataFrame

}

private class SqlStep(config: HoolokStepConfig)
                     (implicit spark: SparkSession) extends Step(config)(spark) {

  val query: String = config.options.flatMap(_.get("query")).getOrElse {
    throw new InvalidConfigException("SQL step is not configured properly. The option 'query' is expected.")
  }

  def processInternal(): DataFrame = {
    logInfo(s"Query to be executed in step: '$query'.")
    spark.sql(query)
  }

}
