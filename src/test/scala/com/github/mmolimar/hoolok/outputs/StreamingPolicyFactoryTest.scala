package com.github.mmolimar.hoolok.outputs

import com.github.mmolimar.hoolok.HoolokSparkTestHarness
import com.github.mmolimar.hoolok.common.InvalidAppConfigException

class StreamingPolicyFactoryTest extends HoolokSparkTestHarness {

  s"a ${StreamingPolicyFactory.getClass.getSimpleName}" when {

    "inspecting outputs" should {
      "fail if the output does not exist" in {
        assertThrows[InvalidAppConfigException] {
          StreamingPolicyFactory("notexist")
        }
      }
      "find the outputs" in {
        StreamingPolicyFactory("default").isInstanceOf[DefaultStreamingPolicy] shouldBe true
        StreamingPolicyFactory("failfast").isInstanceOf[FailfastStreamingPolicy] shouldBe true
      }
    }
  }

}
