package com.github.mmolimar.hoolok.inputs

import com.github.mmolimar.hoolok.HoolokInputConfig
import com.github.mmolimar.hoolok.annotations.InputKind
import com.github.mmolimar.hoolok.common.Implicits.DataStreamReaderWithSchema
import org.apache.spark.sql.{DataFrame, SparkSession}

@InputKind(kind = "stream")
class StreamBasedInput(config: HoolokInputConfig)
                      (implicit spark: SparkSession) extends BaseInput(config)(spark) {

  def readInternal: DataFrame = spark
    .readStream
    .format(config.format)
    .possiblyWithSchema(config.schema)
    .options(config.options.getOrElse(Map.empty))
    .load()

}
