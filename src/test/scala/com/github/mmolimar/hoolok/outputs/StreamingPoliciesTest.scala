package com.github.mmolimar.hoolok.outputs

import com.github.mmolimar.hoolok._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.StreamingQuery

class StreamingPoliciesTest extends HoolokSparkTestHarness {

  import spark.implicits._

  implicit val sqlContext: SQLContext = spark.sqlContext

  s"a ${classOf[DefaultStreamingPolicy].getSimpleName}" when {
    "waiting for streams" should {
      "not wait if there are no active streams" in {
        new DefaultStreamingPolicy().waitForStreams()
      }
      "wait just for one stream" in {
        val streamDF = MemoryStream[String].toDF()
        streamDF.writeStream.outputMode("append").format("console").start()
        stopStreamingQuery(Some(spark.streams.active.head))
        new DefaultStreamingPolicy().waitForStreams()
        spark.streams.active.isEmpty shouldBe true
      }
      "wait just for all active streams" in {
        val streamDF1 = MemoryStream[String].toDF()
        streamDF1.writeStream.outputMode("append").format("console").start()
        val streamDF2 = MemoryStream[String].toDF()
        streamDF2.writeStream.outputMode("append").format("console").start()
        val streamDF3 = MemoryStream[String].toDF()
        streamDF3.writeStream.outputMode("append").format("console").start()
        val streamDF4 = MemoryStream[String].toDF()
        streamDF4.writeStream.outputMode("append").format("console").start()
        stopStreamingQuery()
        new DefaultStreamingPolicy().waitForStreams()
        spark.streams.active.isEmpty shouldBe true
      }
    }
  }

  s"a ${classOf[FailfastStreamingPolicy].getSimpleName}" when {
    "waiting for streams" should {
      "not wait if there are no active streams" in {
        new FailfastStreamingPolicy().waitForStreams()
      }
      "wait just for one stream" in {
        val streamDF = MemoryStream[String].toDF()
        streamDF.writeStream.outputMode("append").format("console").start()
        stopStreamingQuery(Some(spark.streams.active.head))
        new FailfastStreamingPolicy().waitForStreams()
        spark.streams.active.isEmpty shouldBe true
      }
      "wait just for the first active stream and finish" in {
        val streamDF1 = MemoryStream[String].toDF()
        streamDF1.writeStream.outputMode("append").format("console").start()
        val streamDF2 = MemoryStream[String].toDF()
        streamDF2.writeStream.outputMode("append").format("console").start()
        val streamDF3 = MemoryStream[String].toDF()
        streamDF3.writeStream.outputMode("append").format("console").start()
        val streamDF4 = MemoryStream[String].toDF()
        streamDF4.writeStream.outputMode("append").format("console").start()
        stopStreamingQuery()
        new FailfastStreamingPolicy().waitForStreams()
        spark.streams.active.isEmpty shouldBe false
      }
    }
  }

  private def stopStreamingQuery(query: Option[StreamingQuery] = None, waitTime: Long = 5000L): Unit = {
    def stopQuery(q: StreamingQuery): Unit = {
      new Thread() {
        override def run(): Unit = {
          Thread.sleep(waitTime)
          q.stop()
          stopStreamingQuery()
        }
      }.start()
    }

    query.orElse(spark.streams.active.headOption).foreach(stopQuery)
  }

}
