package com.github.mmolimar.hoolok.steps

import com.github.mmolimar.hoolok.HoolokStepConfig
import com.github.mmolimar.hoolok.annotations.StepKind
import org.apache.spark.sql.{DataFrame, SparkSession}

@StepKind(kind = "checkpoint")
class CheckpointStep(config: HoolokStepConfig)
                    (implicit spark: SparkSession) extends DataframeBatchBasedStep(config)(spark) {

  val eager: Boolean = config.options.flatMap(_.get("eager").map(_.toBoolean)).getOrElse(true)
  val reliableCheckpoint: Boolean = config.options.flatMap(_.get("reliableCheckpoint")).exists(_.toBoolean)

  def fromDataframe(dataframe: DataFrame): DataFrame = {
    logInfo(s"Checkpointing dataframe with config: 'eager=$eager' and 'reliableCheckpoint=$reliableCheckpoint'.")
    if (reliableCheckpoint) {
      dataframe.checkpoint(eager)
    } else {
      dataframe.localCheckpoint(eager)
    }
  }

}
