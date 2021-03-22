package com.github.mmolimar.hoolok.schemas

import com.github.mmolimar.hoolok.HoolokSchemaConfig
import com.github.mmolimar.hoolok.annotations.SchemaKind
import com.github.mmolimar.hoolok.common.InvalidSchemaConfigException
import org.apache.spark.sql.SparkSession

@SchemaKind(kind = "inline")
class InlineSchema(config: HoolokSchemaConfig)
                  (implicit spark: SparkSession) extends BaseSchema(config)(spark) {

  val content: String = config.options.flatMap(_.get("value")).getOrElse {
    throw new InvalidSchemaConfigException("Inline schema is not configured properly. The option 'value' is expected.")
  }

  override def schemaContent: String = content

}
