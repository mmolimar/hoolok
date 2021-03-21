package com.github.mmolimar.hoolok.outputs

import com.github.mmolimar.hoolok.HoolokOutputConfig
import com.github.mmolimar.hoolok.annotations.OutputKind
import org.apache.spark.sql.{DataFrame, SparkSession}

@OutputKind(kind = "batch")
class BatchBasedOutput(config: HoolokOutputConfig)
                      (implicit spark: SparkSession) extends BaseOutput(config)(spark) {

  def writeInternal(dataframe: DataFrame): Unit = dataframe
    .write
    .mode(config.mode)
    .format(config.format)
    .partitionBy(config.partitionBy.orNull: _*)
    .options(config.options.getOrElse(Map.empty))
    .save()
}
