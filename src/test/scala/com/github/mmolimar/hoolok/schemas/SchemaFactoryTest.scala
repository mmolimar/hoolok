package com.github.mmolimar.hoolok.schemas

import com.github.mmolimar.hoolok.common.InvalidSchemaConfigException
import com.github.mmolimar.hoolok.{HoolokSchemaConfig, HoolokSparkTestHarness}

class SchemaFactoryTest extends HoolokSparkTestHarness {

  s"a ${SchemaFactory.getClass.getSimpleName}" when {

    "inspecting schemas" should {
      "fail if the schema does not exist" in {
        val config = HoolokSchemaConfig(id = "sample", kind = "invalid", format = "spark-ddl")
        assertThrows[InvalidSchemaConfigException] {
          SchemaFactory(config)
        }
      }
      "find the inputs" in {
        SchemaFactory(inlineSchemaConfig).isInstanceOf[InlineSchema] shouldBe true
        SchemaFactory(uriSchemaConfig).isInstanceOf[UriSchema] shouldBe true
        SchemaFactory(schemaRegistrySchemaConfig).isInstanceOf[SchemaRegistrySchema] shouldBe true
      }
    }
  }

}
