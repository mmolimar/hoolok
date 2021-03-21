package com.github.mmolimar.hoolok.inputs

import com.github.mmolimar.hoolok.HoolokInputConfig
import com.github.mmolimar.hoolok.annotations.InputKind
import com.github.mmolimar.hoolok.common.Implicits.DataFrameReaderWithSchema
import org.apache.spark.sql.{DataFrame, SparkSession}

@InputKind(kind = "batch")
class BatchBasedInput(config: HoolokInputConfig)
                     (implicit spark: SparkSession) extends BaseInput(config)(spark) {

  override def readInternal: DataFrame = spark
    .read
    .format(config.format)
    .possiblyWithSchema(config.schema)
    .options(config.options.getOrElse(Map.empty))
    .load()

}
