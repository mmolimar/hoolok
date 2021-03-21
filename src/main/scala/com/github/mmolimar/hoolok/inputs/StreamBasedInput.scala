package com.github.mmolimar.hoolok.inputs

import com.github.mmolimar.hoolok.HoolokInputConfig
import com.github.mmolimar.hoolok.annotations.InputKind
import org.apache.spark.sql.{DataFrame, SparkSession}

@InputKind(kind = "stream")
private class StreamBasedInput(config: HoolokInputConfig)
                              (implicit spark: SparkSession) extends BaseInput(config)(spark) {

  def readInternal: DataFrame = spark
    .readStream
    .format(config.format)
    .options(config.options.getOrElse(Map.empty))
    .load()

}
