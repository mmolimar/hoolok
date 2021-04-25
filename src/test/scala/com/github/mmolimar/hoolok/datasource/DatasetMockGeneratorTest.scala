package com.github.mmolimar.hoolok.datasource

import com.github.mmolimar.hoolok.HoolokSparkTestHarness
import com.github.mmolimar.hoolok.common.InvalidInputConfigException

class DatasetMockGeneratorTest extends HoolokSparkTestHarness {

  val generator: DatasetMockGenerator = DatasetMockGenerator(Some("es"))
  val generatorLocale: DatasetMockGenerator = DatasetMockGenerator(Some("es"))

  s"a ${classOf[DatasetMockGenerator].getSimpleName}" when {

    "faking a value" should {
      "fail if the faker does not exist" in {
        assertThrows[InvalidInputConfigException] {
          generator.fakeValue(FakerMethod("NotExistentClass", "method"))
        }
        assertThrows[InvalidInputConfigException] {
          generator.fakeValue(FakerMethod("Book", "method"))
        }
        assertThrows[InvalidInputConfigException] {
          generatorLocale.fakeValue(FakerMethod("NotExistentClass", "method"))
        }
        assertThrows[InvalidInputConfigException] {
          generatorLocale.fakeValue(FakerMethod("Book", "method"))
        }
      }
      "return a faked value when the config is right" in {
        generator.fakeValue(FakerMethod("Book", "title")).toString.isEmpty shouldBe false
        generatorLocale.fakeValue(FakerMethod("Book", "title")).toString.isEmpty shouldBe false
      }
    }
    "creating a field" should {
      "fail if the faker does not exist" in {
        assertThrows[InvalidInputConfigException] {
          schema.fields.foreach(generator.createField(_, Map("long" -> FakerMethod("NotExistentClass", "method"))))
        }
        assertThrows[InvalidInputConfigException] {
          schema.fields.foreach(generatorLocale.createField(_, Map("long" -> FakerMethod("NotExistentClass", "method"))))
        }
      }
    }
    "return a faked value when the config is right" in {
      schema.fields.foreach { f =>
        generatorLocale.createField(f, Map("string" -> FakerMethod("Book", "title"))) shouldNot be(None.orNull)
      }
    }
    "creating a dataframe" should {
      "fail if the faker does not exist" in {
        assertThrows[InvalidInputConfigException] {
          generator.createFakeDataframe(spark.sqlContext, 10, schema,
            Map("long" -> FakerMethod("NotExistentClass", "method")))
        }
        assertThrows[InvalidInputConfigException] {
          generatorLocale.createFakeDataframe(spark.sqlContext, 10, schema,
            Map("long" -> FakerMethod("NotExistentClass", "method")))
        }
      }
    }
    "return the dataframe when the config is right" in {
      val df1 = generator.createFakeDataframe(spark.sqlContext, 10, schema,
        Map("string" -> FakerMethod("Book", "title")))
      df1.schema.fields shouldBe schema.fields
      df1.count() shouldBe 10

      val df2 = generatorLocale.createFakeDataframe(spark.sqlContext, 10, schema,
        Map("string" -> FakerMethod("Book", "title")))
      df2.schema.fields shouldBe schema.fields
      df2.count() shouldBe 10
    }

  }
}
