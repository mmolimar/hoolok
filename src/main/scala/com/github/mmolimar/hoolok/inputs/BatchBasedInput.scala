package com.github.mmolimar.hoolok.inputs

import com.github.mmolimar.hoolok.HoolokInputConfig
import com.github.mmolimar.hoolok.annotations.InputBatchKind
import com.github.mmolimar.hoolok.common.Implicits.DataFrameReaderWithSchema
import com.github.mmolimar.hoolok.common.InvalidConfigException
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

abstract class BatchBasedInput(config: HoolokInputConfig)
                              (implicit spark: SparkSession) extends BaseInput(config)(spark) {

  override final def readInternal: DataFrame = load(
    spark
      .read
      .format(config.format)
      .possiblyWithSchema(config.schema)
      .options(config.options.getOrElse(Map.empty))
  )

  protected def load(dfr: DataFrameReader): DataFrame
}

@InputBatchKind(subtype = "table")
class TableBatchInput(config: HoolokInputConfig)
                     (implicit spark: SparkSession) extends BatchBasedInput(config)(spark) {

  val tableName: String = config.options.flatMap(_.get("tableName")).getOrElse {
    throw new InvalidConfigException(s"Table batch input for ID '${config.id}' is not configured properly. " +
      "The option 'tableName' is expected.")
  }

  override def load(dfr: DataFrameReader): DataFrame = dfr.table(tableName)

}

@InputBatchKind(subtype = "data-source")
class DataSourceBatchInput(config: HoolokInputConfig)
                          (implicit spark: SparkSession) extends BatchBasedInput(config)(spark) {

  override def load(dfr: DataFrameReader): DataFrame = dfr.load()

}
