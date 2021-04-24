package com.github.mmolimar.hoolok.steps

import com.github.mmolimar.hoolok.common.InvalidStepConfigException
import com.github.mmolimar.hoolok.{HoolokSparkTestHarness, HoolokStepConfig}

class StepFactoryTest extends HoolokSparkTestHarness {

  s"a ${StepFactory.getClass.getSimpleName}" when {
    "inspecting steps" should {

      "fail if the step does not exist" in {
        val config = HoolokStepConfig(id = "sample", kind = "invalid", options = None, dq = None)
        assertThrows[InvalidStepConfigException] {
          StepFactory(config)
        }
      }
      "find the steps" in {
        StepFactory(cacheConfig).isInstanceOf[CacheStep] shouldBe true
        StepFactory(checkpointConfig).isInstanceOf[CheckpointStep] shouldBe true
        StepFactory(fromAvroConfig).isInstanceOf[FromAvroStep] shouldBe true
        StepFactory(fromJsonConfig).isInstanceOf[FromJsonStep] shouldBe true
        StepFactory(showConfig).isInstanceOf[ShowStep] shouldBe true
        StepFactory(sqlConfig).isInstanceOf[SqlStep] shouldBe true
        StepFactory(toAvroConfig).isInstanceOf[ToAvroStep] shouldBe true
        StepFactory(unpersistConfig).isInstanceOf[UnpersistStep] shouldBe true
      }
    }
  }

}
