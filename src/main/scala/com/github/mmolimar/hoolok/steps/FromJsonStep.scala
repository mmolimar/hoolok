package com.github.mmolimar.hoolok.steps

import com.github.mmolimar.hoolok.HoolokStepConfig
import com.github.mmolimar.hoolok.annotations.StepKind
import com.github.mmolimar.hoolok.common.InvalidStepConfigException
import com.github.mmolimar.hoolok.schemas.SchemaManager
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.{ColumnName, DataFrame, SparkSession}

@StepKind(kind = "from-json")
class FromJsonStep(config: HoolokStepConfig)
                  (implicit spark: SparkSession) extends BaseStep(config)(spark) {

  val dataframe: String = config.options.flatMap(_.get("dataframe")).getOrElse {
    throw new InvalidStepConfigException("FromJson step is not configured properly. The option 'dataframe' is expected.")
  }
  val columns: Array[ColumnName] = config.options.flatMap(_.get("columns")
    .map(_.split(",").map(cn => new ColumnName(cn.trim))))
    .getOrElse {
      throw new InvalidStepConfigException("FromJson step is not configured properly. The option 'columns' is expected.")
    }
  val alias: Array[String] = config.options.flatMap(_.get("alias")
    .map(_.split(",").map(_.trim))
  ).getOrElse {
    throw new InvalidStepConfigException("FromJson step is not configured properly. The option 'alias' is expected.")
  }
  if (columns.length != alias.length) {
    throw new InvalidStepConfigException("FromJson step is not configured properly. " +
      "The option 'columns' and 'alias' must have the same length.")
  }
  val schema: String = config.options.flatMap(_.get("schema")).getOrElse {
    throw new InvalidStepConfigException("FromJson step is not configured properly. The option 'schema' is expected.")
  }
  val selectFields: Option[Array[ColumnName]] = config.options
    .flatMap(_.get("select").map(_.split(",").map(cn => new ColumnName(cn.trim))))

  def processInternal(): DataFrame = {
    val sparkSchema = SchemaManager.getSchema(schema)
    val selection = new ColumnName("*") +: columns.zipWithIndex.map {
      case (col, index) => from_json(col, sparkSchema).as(alias(index))
    }
    val df = spark.table(dataframe).select(selection: _*)
    selectFields.map(s => df.select(s: _*)).getOrElse(df)
  }

}
