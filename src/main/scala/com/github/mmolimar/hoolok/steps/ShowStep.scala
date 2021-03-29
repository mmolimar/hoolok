package com.github.mmolimar.hoolok.steps

import com.github.mmolimar.hoolok.HoolokStepConfig
import com.github.mmolimar.hoolok.annotations.StepKind
import org.apache.spark.sql.{DataFrame, SparkSession}

@StepKind(kind = "show")
class ShowStep(config: HoolokStepConfig)
              (implicit spark: SparkSession) extends DataframeBatchBasedStep(config)(spark) {

  val numRows: Int = config.options.flatMap(_.get("numRows")).getOrElse("20").toInt
  val truncate: Boolean = config.options.flatMap(_.get("truncate")).getOrElse("true").toBoolean

  def fromDataframe(dataframe: DataFrame): DataFrame = {
    logInfo(s"Content of the dataframe with ID '${config.id}'.")
    dataframe.show(numRows = numRows, truncate = truncate)
    dataframe
  }

}
