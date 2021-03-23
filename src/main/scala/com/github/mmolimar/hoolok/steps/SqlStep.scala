package com.github.mmolimar.hoolok.steps

import com.github.mmolimar.hoolok.HoolokStepConfig
import com.github.mmolimar.hoolok.annotations.StepKind
import com.github.mmolimar.hoolok.common.InvalidStepConfigException
import org.apache.spark.sql.{DataFrame, SparkSession}

@StepKind(kind = "sql")
class SqlStep(config: HoolokStepConfig)
             (implicit spark: SparkSession) extends BaseStep(config)(spark) {

  val query: String = config.options.flatMap(_.get("query")).getOrElse {
    throw new InvalidStepConfigException("SQL step is not configured properly. The option 'query' is expected.")
  }

  def processInternal(): DataFrame = {
    logInfo(s"Query to be executed in step: '$query'.")
    spark.sql(query)
  }

}
