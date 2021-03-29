package com.github.mmolimar.hoolok.steps

import com.github.mmolimar.hoolok.HoolokStepConfig
import com.github.mmolimar.hoolok.annotations.StepKind
import org.apache.spark.sql.{DataFrame, SparkSession}

@StepKind(kind = "unpersist")
class UnpersistStep(config: HoolokStepConfig)
                   (implicit spark: SparkSession) extends DataframeBatchBasedStep(config)(spark) {

  val blocking: Boolean = config.options.exists(_.get("blocking").exists(_.trim.toBoolean))

  def fromDataframe(dataframe: DataFrame): DataFrame = {
    logInfo(s"Unpersisting dataframe with blocking '$blocking'.")
    dataframe.unpersist(blocking)
  }

}
