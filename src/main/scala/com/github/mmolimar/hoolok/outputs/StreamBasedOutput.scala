package com.github.mmolimar.hoolok.outputs

import com.github.mmolimar.hoolok.HoolokOutputConfig
import com.github.mmolimar.hoolok.annotations.OutputKind
import org.apache.spark.sql.{DataFrame, SparkSession}

@OutputKind(kind = "stream")
class StreamBasedOutput(config: HoolokOutputConfig)
                           (implicit spark: SparkSession) extends BaseOutput(config)(spark) {

  def writeInternal(dataframe: DataFrame): Unit = dataframe
    .writeStream
    .outputMode(config.mode)
    .format(config.format)
    .options(config.options.getOrElse(Map.empty))
    .partitionBy(config.partitionBy.orNull: _*)
    .start()
    .awaitTermination()
}
