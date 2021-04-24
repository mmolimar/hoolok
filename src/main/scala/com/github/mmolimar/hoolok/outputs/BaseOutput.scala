package com.github.mmolimar.hoolok.outputs

import com.github.mmolimar.hoolok.HoolokOutputConfig
import com.github.mmolimar.hoolok.common.Implicits.DataframeEnricher
import com.github.mmolimar.hoolok.common.{InvalidOutputConfigException, SchemaValidationException}
import com.github.mmolimar.hoolok.schemas.SchemaManager
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class BaseOutput(override val config: HoolokOutputConfig)
                         (implicit spark: SparkSession) extends Output with Logging {

  override final def write(): Unit = {
    logInfo(s"Writing output '${config.kind}' for ID '${config.id}' with format '${config.format}'.")
    val dataframe = spark.table(config.id)
      .possiblyWithCoalesce(config.coalesce)
      .possiblyWithRepartition(config.repartition)
      .possiblyWithWatermark(config.watermark)

    config.schema.foreach(s => validate(dataframe, SchemaManager.getSchema(s).getOrElse(
      throw new InvalidOutputConfigException(s"Schema '$s' does not exist.")
    )))
    writeInternal(dataframe)
  }

  private def validate(dataframe: DataFrame, schema: StructType): Unit = {
    def validateFields(dfFields: Array[StructField], schemaFields: Array[StructField]): Boolean = {
      dfFields.forall { sf =>
        sf.dataType match {
          case s: StructType =>
            schemaFields.find(f => f.name == sf.name && f.dataType == sf.dataType)
              .exists(field => validateFields(s.fields, field.dataType.asInstanceOf[StructType].fields))
          case a: ArrayType => schemaFields.find(f => f.name == sf.name && f.dataType == sf.dataType)
            .map(_.dataType.asInstanceOf[ArrayType])
            .exists(array => array.elementType == a.elementType && array.containsNull == a.containsNull)
          case _ => schemaFields.exists(f => f.name == sf.name && f.dataType == sf.dataType)
        }
      }
    }

    if (!validateFields(dataframe.schema.fields, schema.fields)) {
      throw new SchemaValidationException(s"Dataframe with id '${config.id}' does not match " +
        s"with the schema '${config.schema.getOrElse("")}'.")
    }
    logInfo(s"Dataframe with ID '${config.id}' has been validated successfully " +
      s"against schema ID '${config.schema.getOrElse("")}'.")
  }

  protected def writeInternal(dataframe: DataFrame): Unit

}
