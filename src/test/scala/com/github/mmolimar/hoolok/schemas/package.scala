package com.github.mmolimar.hoolok

import org.apache.spark.sql.types.StructType

package object schemas {

  val inlineSchemaConfig: HoolokSchemaConfig = HoolokSchemaConfig(
    id = "inline-schema",
    kind = "inline",
    format = "spark-ddl",
    options = Some(Map("value" -> "col1 INT, col2 STRING, col3 STRING, col4 BINARY, col5 BINARY, col6 BOOLEAN"))
  )
  val uriSchemaConfig: HoolokSchemaConfig = HoolokSchemaConfig(
    id = "uri-schema1",
    kind = "uri",
    format = "json-schema",
    options = Some(Map("path" -> "/schemas/test-schema.json"))
  )
  val schemaRegistrySchemaConfig: HoolokSchemaConfig = HoolokSchemaConfig(
    id = "schema-registry-schema1",
    kind = "schema-registry",
    format = "avro-schema",
    options = Some(Map("schemaRegistryUrls" -> "http://localhost:47774", "subject" -> "test-subject"))
  )

  def registerSchemas(): Unit = {
    val schemaContent = "col1 INT, col2 STRING, col3 STRING, col4 BINARY, col5 BINARY, col6 BOOLEAN"
    SchemaManager.registerSchema("schema-test", StructType.fromDDL(schemaContent))

    val schemaContentJson = "field1 STRING, field2 STRING"
    SchemaManager.registerSchema("schema-test-json", StructType.fromDDL(schemaContentJson))

    val schemaContentAvro = "field1 STRING, field2 STRING"
    SchemaManager.registerSchema("schema-test-avro", StructType.fromDDL(schemaContentAvro))
  }

}
