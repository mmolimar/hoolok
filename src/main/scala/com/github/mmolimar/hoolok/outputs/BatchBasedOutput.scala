package com.github.mmolimar.hoolok.outputs

import com.github.mmolimar.hoolok.HoolokOutputConfig
import com.github.mmolimar.hoolok.annotations.OutputBatchKind
import com.github.mmolimar.hoolok.common.Implicits.DataframeEnricher
import com.github.mmolimar.hoolok.common.InvalidOutputConfigException
import org.apache.spark.sql.{DataFrame, DataFrameWriter, SparkSession}

abstract class BatchBasedOutput(config: HoolokOutputConfig)
                               (implicit spark: SparkSession) extends BaseOutput(config)(spark) {

  override final def writeInternal(dataframe: DataFrame): Unit = save(
    dataframe
      .possiblyWithDataQuality(config.kind, config.id, config.dq)
      .write
      .mode(config.mode)
      .format(config.format)
      .partitionBy(config.partitionBy.orNull: _*)
      .options(config.options.getOrElse(Map.empty))
  )

  protected def save[T](dfr: DataFrameWriter[T]): Unit
}

@OutputBatchKind(subtype = "table")
class TableBatchOutput(config: HoolokOutputConfig)
                      (implicit spark: SparkSession) extends BatchBasedOutput(config)(spark) {

  val tableName: String = config.options.flatMap(_.get("tableName")).getOrElse {
    throw new InvalidOutputConfigException("Table output is not configured properly. The option 'tableName' is expected.")
  }

  protected def save[T](dfr: DataFrameWriter[T]): Unit = dfr.saveAsTable(tableName)

}

@OutputBatchKind(subtype = "data-source")
class DataSourceBatchOutput(config: HoolokOutputConfig)
                           (implicit spark: SparkSession) extends BatchBasedOutput(config)(spark) {

  protected def save[T](dfr: DataFrameWriter[T]): Unit = dfr.save()

}
