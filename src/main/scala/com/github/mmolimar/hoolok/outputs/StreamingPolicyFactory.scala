package com.github.mmolimar.hoolok.outputs

import com.github.mmolimar.hoolok.common.InvalidAppConfigException
import com.github.mmolimar.hoolok.common.Utils.inspectStreamingPolicies
import org.apache.spark.sql.SparkSession

trait StreamingPolicy {

  def waitForStreams(): Unit

}

object StreamingPolicyFactory {

  private val streamingPolicies = inspectStreamingPolicies

  def apply(kind: String)(implicit spark: SparkSession): StreamingPolicy = {
    streamingPolicies.get(kind.trim.toLowerCase)
      .map(clz => clz.getConstructor(classOf[SparkSession]))
      .map(_.newInstance(spark))
      .getOrElse {
        throw new InvalidAppConfigException(s"Streaming policy kind '$kind' is not supported.")
      }
  }

}
