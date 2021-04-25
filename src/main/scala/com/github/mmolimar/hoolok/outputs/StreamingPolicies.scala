package com.github.mmolimar.hoolok.outputs

import com.github.mmolimar.hoolok.annotations.StreamingPolicyKind
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

abstract class AbstractStreamingPolicy(implicit spark: SparkSession) extends StreamingPolicy with Logging {

  override final def waitForStreams(): Unit = {
    if (spark.streams.active.nonEmpty) {
      internalWaitForStreams()
    }
  }

  protected def internalWaitForStreams(): Unit

}

@StreamingPolicyKind(kind = "default")
class DefaultStreamingPolicy(implicit spark: SparkSession) extends AbstractStreamingPolicy()(spark) {

  final override protected def internalWaitForStreams(): Unit = {
    spark.streams.awaitAnyTermination()
    spark.streams.resetTerminated()
    waitForStreams()
  }

}

@StreamingPolicyKind(kind = "failfast")
class FailfastStreamingPolicy(implicit spark: SparkSession) extends AbstractStreamingPolicy()(spark) {

  final override protected def internalWaitForStreams(): Unit = spark.streams.awaitAnyTermination()

}
