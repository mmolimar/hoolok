package com.github.mmolimar.hoolok.outputs

import com.github.mmolimar.hoolok.common.InvalidOutputConfigException
import com.github.mmolimar.hoolok.{HoolokOutputConfig, HoolokSparkTestHarness}

class OutputFactoryTest extends HoolokSparkTestHarness {

  s"a ${OutputFactory.getClass.getSimpleName}" when {

    "inspecting outputs" should {
      "fail if the output does not exist" in {
        val config = HoolokOutputConfig(id = "sample", kind = "csv", format = "csv", mode = "append", subtype = "data-source")
        assertThrows[InvalidOutputConfigException] {
          OutputFactory(config)
        }
      }
      "find the outputs" in {
        OutputFactory(tableBatchOutputConfig).isInstanceOf[TableBatchOutput] shouldBe true
        OutputFactory(dataSourceBatchOutputConfig).isInstanceOf[DataSourceBatchOutput] shouldBe true
        OutputFactory(tableStreamOutputConfig).isInstanceOf[TableStreamOutput] shouldBe true
        OutputFactory(dataSourceStreamOutputConfig).isInstanceOf[DataSourceStreamOutput] shouldBe true
      }
    }
  }

}
