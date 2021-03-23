package com.github.mmolimar.hoolok.steps

import com.github.mmolimar.hoolok.HoolokStepConfig
import com.github.mmolimar.hoolok.annotations.StepKind
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

@StepKind(kind = "cache")
class CacheStep(config: HoolokStepConfig)
               (implicit spark: SparkSession) extends DataframeBasedStep(config)(spark) {

  val storageLevel: StorageLevel = config.options.flatMap(_.get("storageLevel")
    .map(_.trim.toUpperCase).map(StorageLevel.fromString)).getOrElse(StorageLevel.MEMORY_AND_DISK)

  def fromDataframe(dataframe: DataFrame): DataFrame = {
    logInfo(s"Caching dataframe with storage level '$storageLevel'.")
    dataframe.persist(storageLevel)
  }

}
