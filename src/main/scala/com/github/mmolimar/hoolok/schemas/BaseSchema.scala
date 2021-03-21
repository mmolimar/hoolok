package com.github.mmolimar.hoolok.schemas

import com.github.mmolimar.hoolok.HoolokSchemaConfig
import com.github.mmolimar.hoolok.common.InvalidSchemaConfigException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

abstract class BaseSchema(override val config: HoolokSchemaConfig)
                         (implicit spark: SparkSession) extends Schema with Logging {

  override final def register(): Unit = {
    val schema: StructType = config.format.trim.toLowerCase match {
      case "ddl" => StructType.fromDDL(schemaContent)
      case _ => throw new InvalidSchemaConfigException(s"Schema format '${config.format}' is not supported.")
    }
    SchemaManager.registerSchema(config.id, schema)
  }

  def schemaContent: String

}
