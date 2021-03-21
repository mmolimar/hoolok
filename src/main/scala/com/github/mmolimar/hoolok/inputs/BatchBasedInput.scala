package com.github.mmolimar.hoolok.inputs

import com.github.mmolimar.hoolok.HoolokInputConfig
import com.github.mmolimar.hoolok.annotations.InputKind
import org.apache.spark.sql.{DataFrame, SparkSession}

@InputKind(kind = "batch")
private class BatchBasedInput(config: HoolokInputConfig)
                             (implicit spark: SparkSession) extends BaseInput(config)(spark) {

  override def readInternal: DataFrame = spark
    .read
    .format(config.format)
    .options(config.options.getOrElse(Map.empty))
    .load()

}
