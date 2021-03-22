package com.github.mmolimar.hoolok.schemas

import com.github.mmolimar.hoolok.HoolokSchemaConfig
import com.github.mmolimar.hoolok.common.InvalidSchemaConfigException
import org.apache.avro.{Schema => AvroSchema}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.types.StructType
import org.zalando.spark.jsonschema.SchemaConverter

abstract class BaseSchema(override val config: HoolokSchemaConfig)
                         (implicit spark: SparkSession) extends Schema with Logging {

  override final def register(): Unit = {
    val schema: StructType = config.format.trim.toLowerCase match {
      case "spark-ddl" => StructType.fromDDL(schemaContent)
      case "json-schema" => SchemaConverter.convertContent(schemaContent)
      case "avro-schema" => SchemaConverters.toSqlType(new AvroSchema.Parser().parse(schemaContent))
        .dataType.asInstanceOf[StructType]
      case _ => throw new InvalidSchemaConfigException(s"Schema format '${config.format}' is not supported.")
    }
    SchemaManager.registerSchema(config.id, schema)
  }

  def schemaContent: String

}
