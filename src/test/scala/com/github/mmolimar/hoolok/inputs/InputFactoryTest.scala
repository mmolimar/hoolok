package com.github.mmolimar.hoolok.inputs

import com.github.mmolimar.hoolok.common.InvalidInputConfigException
import com.github.mmolimar.hoolok.{HoolokInputConfig, HoolokSparkTestHarness}

class InputFactoryTest extends HoolokSparkTestHarness {

  s"a ${InputFactory.getClass.getSimpleName}" when {

    "inspecting inputs" should {
      "fail if the input does not exist" in {
        val config = HoolokInputConfig(id = "sample", kind = "csv", format = "csv", subtype = "data-source")
        assertThrows[InvalidInputConfigException] {
          InputFactory(config)
        }
      }
      "find the inputs" in {
        InputFactory(tableBatchInputConfig).isInstanceOf[TableBatchInput] shouldBe true
        InputFactory(dataSourceBatchInputConfig).isInstanceOf[DataSourceBatchInput] shouldBe true
        InputFactory(tableStreamInputConfig).isInstanceOf[TableStreamInput] shouldBe true
        InputFactory(dataSourceStreamInputConfig).isInstanceOf[DataSourceStreamInput] shouldBe true
      }
    }
  }

}
