package com.github.mmolimar.hoolok.datasource

import com.github.mmolimar.hoolok.HoolokSparkTestHarness
import com.github.mmolimar.hoolok.common.InvalidInputConfigException
import org.apache.spark.sql.execution.streaming.LongOffset

class DefaultSourceTest extends HoolokSparkTestHarness {

  s"a ${classOf[DefaultSource].getSimpleName}" when {
    "creating a relation" should {
      "fail if there is no schema" in {
        assertThrows[InvalidInputConfigException] {
          new DefaultSource().createRelation(spark.sqlContext, Map.empty)
        }
      }
      "return the mock relation with no fakers" in {
        val relation = new DefaultSource().createRelation(spark.sqlContext, Map.empty, schema)
        relation.isInstanceOf[HoolokMockRelation] shouldBe true
        val mockRelation = relation.asInstanceOf[HoolokMockRelation]
        val rdd = mockRelation.buildScan()
        rdd.count shouldBe 20
      }
      "return the mock relation with fakers" in {
        val relation = new DefaultSource().createRelation(spark.sqlContext, parameters, schema)
        relation.isInstanceOf[HoolokMockRelation] shouldBe true
        val mockRelation = relation.asInstanceOf[HoolokMockRelation]
        val rdd = mockRelation.buildScan()
        rdd.count shouldBe 10
      }
    }
    "creating a source" should {
      "fail if there is no schema" in {
        assertThrows[InvalidInputConfigException] {
          new DefaultSource().createSource(spark.sqlContext, "", None, "", Map.empty)
        }
      }
      "fail if the fakers are invalid" in {
        val params1 = Map("fakers" -> "course_title:Invalid.method")
        assertThrows[InvalidInputConfigException] {
          new DefaultSource().createSource(spark.sqlContext, "", None, "", params1)
        }
        val params2 = Map("fakers" -> "course_title:Book.method")
        assertThrows[InvalidInputConfigException] {
          new DefaultSource().createSource(spark.sqlContext, "", None, "", params2)
        }
      }
      "return the mock relation with no fakers" in {
        val source = new DefaultSource()
          .createSource(spark.sqlContext, "", Some(schema), "", Map.empty)
        source.isInstanceOf[HoolokMockSource] shouldBe true
        val mockSource = source.asInstanceOf[HoolokMockSource]
        mockSource.getOffset shouldBe Some(LongOffset(0L))
        val df = mockSource.getBatch(None, LongOffset(0L))
        mockSource.getOffset shouldBe Some(LongOffset(1L))
        df.isStreaming shouldBe true
        mockSource.stop()
        println(mockSource.toString)
      }
      "return the mock relation with fakers" in {
        val source = new DefaultSource()
          .createSource(spark.sqlContext, "", Some(schema), "", parameters)
        source.isInstanceOf[HoolokMockSource] shouldBe true
        val mockSource = source.asInstanceOf[HoolokMockSource]
        mockSource.getOffset shouldBe Some(LongOffset(0L))
        val df = mockSource.getBatch(None, LongOffset(0L))
        mockSource.getOffset shouldBe Some(LongOffset(1L))
        df.isStreaming shouldBe true
        mockSource.stop()
        println(mockSource.toString)
      }
    }
  }
}
