package com.github.mmolimar.hoolok.steps

import com.github.mmolimar.hoolok.HoolokStepConfig
import com.github.mmolimar.hoolok.annotations.StepKind
import com.github.mmolimar.hoolok.common.InvalidStepConfigException
import com.github.mmolimar.hoolok.schemas.SchemaManager
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.{ColumnName, DataFrame, SparkSession}
import za.co.absa.abris.avro.functions.to_avro

@StepKind(kind = "to-avro")
class ToAvroStep(config: HoolokStepConfig)
                (implicit spark: SparkSession) extends BaseStep(config)(spark) {

  val dataframe: String = config.options.flatMap(_.get("dataframe")).getOrElse {
    throw new InvalidStepConfigException("ToAvro step is not configured properly. The option 'dataframe' is expected.")
  }
  val columns: Array[ColumnName] = config.options.flatMap(_.get("columns")
    .map(_.split(",").map(cn => new ColumnName(cn.trim))))
    .getOrElse {
      throw new InvalidStepConfigException("ToAvro step is not configured properly. The option 'columns' is expected.")
    }
  val alias: String = config.options.flatMap(_.get("alias")).getOrElse {
    throw new InvalidStepConfigException("ToAvro step is not configured properly. The option 'alias' is expected.")
  }
  val schema: String = config.options.flatMap(_.get("schema")).getOrElse {
    throw new InvalidStepConfigException("ToAvro step is not configured properly. The option 'schema' is expected.")
  }
  val selectFields: Option[Array[ColumnName]] = config.options
    .flatMap(_.get("select").map(_.split(",").map(cn => new ColumnName(cn.trim))))

  def processInternal(): DataFrame = {
    val sparkSchema = SchemaManager.getSchema(schema)
    val df = spark.table(dataframe)
      .select(
        new ColumnName("*"),
        to_avro(struct(columns: _*), SchemaConverters.toAvroType(sparkSchema).toString).as(alias)
      )
    selectFields.map(s => df.select(s: _*)).getOrElse(df)
  }

}
