package com.github.mmolimar.hoolok.inputs

import com.github.mmolimar.hoolok.HoolokInputConfig
import com.github.mmolimar.hoolok.annotations.InputStreamKind
import com.github.mmolimar.hoolok.common.Implicits.DataStreamReaderWithSchema
import com.github.mmolimar.hoolok.common.InvalidInputConfigException
import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class StreamBasedInput(config: HoolokInputConfig)
                               (implicit spark: SparkSession) extends BaseInput(config)(spark) {

  def readInternal: DataFrame = load(
    spark
      .readStream
      .format(config.format)
      .possiblyWithSchema(config.schema)
      .options(config.options.getOrElse(Map.empty))
  )

  def load(dfr: DataStreamReader): DataFrame

}

@InputStreamKind(subtype = "table")
class TableStreamInput(config: HoolokInputConfig)
                      (implicit spark: SparkSession) extends StreamBasedInput(config)(spark) {

  val tableName: String = config.options.flatMap(_.get("tableName")).getOrElse {
    throw new InvalidInputConfigException(s"Table stream input for ID '${config.id}' is not configured properly. " +
      "The option 'tableName' is expected.")
  }

  override def load(dfr: DataStreamReader): DataFrame = dfr.table(tableName)

}

@InputStreamKind(subtype = "data-source")
class DataSourceStreamInput(config: HoolokInputConfig)
                           (implicit spark: SparkSession) extends StreamBasedInput(config)(spark) {

  override def load(dfr: DataStreamReader): DataFrame = dfr.load()

}
