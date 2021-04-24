package com.github.mmolimar.hoolok

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import java.nio.file.Files

class HoolokSparkTestHarness extends AnyWordSpecLike with Matchers with BeforeAndAfterAll {

  implicit lazy val spark: SparkSession = SparkSession.builder()
    .appName(s"Tests for ${getClass.getName}")
    .master("local")
    .config("spark.sql.warehouse.dir", Files.createTempDirectory("hoolok_warehouse_").toUri.toString)
    .config("spark.sql.streaming.checkpointLocation", Files.createTempDirectory("hoolok_warehouse_").toUri.toString)
    .getOrCreate()

  override def beforeAll(): Unit = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    spark.sparkContext.setCheckpointDir(Files.createTempDirectory("hoolok_").toFile.getAbsolutePath)
    spark.catalog.listTables().collect().foreach(table => spark.catalog.dropTempView(table.name))
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

}
