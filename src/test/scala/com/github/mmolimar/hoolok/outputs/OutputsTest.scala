package com.github.mmolimar.hoolok.outputs

import com.github.mmolimar.hoolok._
import com.github.mmolimar.hoolok.common.{DataQualityValidationException, InvalidOutputConfigException, SchemaValidationException}
import com.github.mmolimar.hoolok.schemas.registerSchemas
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SQLContext}

import java.nio.file.Files
import scala.collection.JavaConverters._

class OutputsTest extends HoolokSparkTestHarness {

  import spark.implicits._

  implicit val sqlContext: SQLContext = spark.sqlContext

  val rows = List(
    Row(1, """{"field1":"value1"}""", """{"field2":"value2"}""", None.orNull, None.orNull, true),
    Row(2, """{"field1":"value1"}""", """{"field2":"value2"}""", None.orNull, None.orNull, false)
  )
  val schema = List(
    StructField("col1", IntegerType),
    StructField("col2", StringType),
    StructField("col3", StringType),
    StructField("col4", BinaryType),
    StructField("col5", BinaryType),
    StructField("col6", BooleanType)
  )
  val testDF: DataFrame = spark.createDataFrame(rows.asJava, StructType(schema))
  registerSchemas()

  val failedDataQualityConfig: HoolokDataQualityConfig = HoolokDataQualityConfig(
    analysis = None,
    verification = Some(HoolokDataQualityVerificationConfig(
      name = "test_verification",
      checks = List(
        HoolokDataQualityCheckConfig(
          level = "error",
          description = "desc",
          hasSize = Some(HoolokDataQualityCheckHasSizeConfig(op = ">", value = 2))
        )
      )
    ))
  )
  val successDataQualityConfig: HoolokDataQualityConfig = HoolokDataQualityConfig(
    analysis = None,
    verification = Some(HoolokDataQualityVerificationConfig(
      name = "test_verification",
      checks = List(
        HoolokDataQualityCheckConfig(
          level = "error",
          description = "desc",
          hasSize = Some(HoolokDataQualityCheckHasSizeConfig(op = "<=", value = 2))
        )
      )
    ))
  )

  s"a ${classOf[TableBatchOutput].getSimpleName}" when {
    val output = new TableBatchOutput(tableBatchOutputConfig)
    val tableName = tableBatchOutputConfig.options.get("tableName")

    "initializing" should {
      "fail if there are no options" in {
        assertThrows[InvalidOutputConfigException] {
          new TableBatchOutput(tableBatchOutputConfig.copy(options = None))
        }
      }
      "set table name" in {
        output.tableName shouldBe "batch_table"
      }
    }
    "writing" should {
      "fail if the data quality validation does not pass" in {
        testDF.createOrReplaceTempView(tableBatchOutputConfig.id)
        spark.catalog.tableExists(tableBatchOutputConfig.id) shouldBe true
        val df = spark.table(tableBatchOutputConfig.id)
        df.schema shouldBe testDF.schema
        assertThrows[DataQualityValidationException] {
          new TableBatchOutput(tableBatchOutputConfig.copy(dq = Some(failedDataQualityConfig))).write()
        }
        spark.catalog.dropTempView(tableBatchOutputConfig.id)
      }
      "save the table" in {
        testDF.createOrReplaceTempView(tableBatchOutputConfig.id)
        spark.catalog.tableExists(tableBatchOutputConfig.id) shouldBe true
        spark.catalog.tableExists(tableName) shouldBe false
        val df = spark.table(tableBatchOutputConfig.id)
        df.schema shouldBe testDF.schema
        output.write()
        spark.catalog.tableExists(tableName) shouldBe true
        spark.catalog.dropTempView(tableBatchOutputConfig.id)
        spark.catalog.dropTempView(tableName)
      }
      "save the table after applying data quality" in {
        testDF.createOrReplaceTempView(tableBatchOutputConfig.id)
        spark.catalog.tableExists(tableBatchOutputConfig.id) shouldBe true
        val df = spark.table(tableBatchOutputConfig.id)
        df.schema shouldBe testDF.schema
        new TableBatchOutput(tableBatchOutputConfig.copy(dq = Some(successDataQualityConfig))).write()
        spark.catalog.dropTempView(tableBatchOutputConfig.id)
        spark.catalog.dropTempView(tableName)
      }
    }
  }

  s"a ${classOf[TableStreamOutput].getSimpleName}" when {
    val output = new TableStreamOutput(tableStreamOutputConfig)
    val tableName = tableStreamOutputConfig.options.get("tableName")
    val streamDF = MemoryStream[String].toDF()

    "initializing" should {
      "fail if there are no options" in {
        assertThrows[InvalidOutputConfigException] {
          new TableStreamOutput(tableStreamOutputConfig.copy(options = None))
        }
      }
      "have default properties" in {
        val output = new DataSourceStreamOutput(dataSourceStreamOutputConfig.copy(options = None))
        output.gracefulShutdownPath.isDefined shouldBe false
        output.gracefulShutdownDelay shouldBe 10
        output.gracefulShutdownFileNotCreated shouldBe true
      }
      "set the properties" in {
        output.tableName shouldBe "stream_table"
        output.gracefulShutdownPath.isDefined shouldBe true
        output.gracefulShutdownDelay shouldBe 30
        output.gracefulShutdownFileNotCreated shouldBe false
      }
    }
    "writing" should {
      "save the table" in {
        streamDF.createOrReplaceTempView(tableStreamOutputConfig.id)
        spark.catalog.tableExists(tableStreamOutputConfig.id) shouldBe true
        spark.catalog.tableExists(tableName) shouldBe false
        val df = spark.table(tableStreamOutputConfig.id)
        df.schema shouldBe streamDF.schema
        output.write()
        spark.catalog.tableExists(tableName) shouldBe true
        spark.catalog.dropTempView(tableStreamOutputConfig.id)
        spark.catalog.dropTempView(tableName)
      }
    }
  }

  s"a ${classOf[DataSourceBatchOutput].getSimpleName}" when {
    val output = new DataSourceBatchOutput(dataSourceBatchOutputConfig.copy(
      options = Some(Map("path" -> Files.createTempDirectory("hoolok_data_source_batch_output_").toUri.toString))
    ))
    "writing" should {
      "fail if cannot infer the schema" in {
        assertThrows[AnalysisException] {
          new DataSourceBatchOutput(dataSourceBatchOutputConfig.copy(schema = None)).write()
        }
      }
      "fail if the schema does not exist" in {
        val output = new DataSourceBatchOutput(dataSourceBatchOutputConfig.copy(schema = Some("notexist")))
        testDF.createOrReplaceTempView(dataSourceBatchOutputConfig.id)
        spark.catalog.tableExists(dataSourceBatchOutputConfig.id) shouldBe true
        assertThrows[InvalidOutputConfigException] {
          output.write()
        }
        spark.catalog.dropTempView(dataSourceBatchOutputConfig.id)
      }
      "fail if the schemas cannot be matched" in {
        val output = new DataSourceBatchOutput(dataSourceBatchOutputConfig.copy(
          options = Some(Map("path" -> Files.createTempDirectory("hoolok_data_source_batch_output_").toUri.toString)),
          schema = Some("schema-test-json")
        ))
        testDF.createOrReplaceTempView(dataSourceBatchOutputConfig.id)
        spark.catalog.tableExists(dataSourceBatchOutputConfig.id) shouldBe true
        val df = spark.table(dataSourceBatchOutputConfig.id)
        df.schema shouldBe testDF.schema
        assertThrows[SchemaValidationException] {
          output.write()
        }
        spark.catalog.dropTempView(dataSourceBatchOutputConfig.id)
      }
      "fail if the data quality validation does not pass" in {
        testDF.createOrReplaceTempView(dataSourceBatchOutputConfig.id)
        spark.catalog.tableExists(dataSourceBatchOutputConfig.id) shouldBe true
        val df = spark.table(dataSourceBatchOutputConfig.id)
        df.schema shouldBe testDF.schema
        assertThrows[DataQualityValidationException] {
          new DataSourceBatchOutput(dataSourceBatchOutputConfig.copy(dq = Some(failedDataQualityConfig))).write()
        }
        spark.catalog.dropTempView(dataSourceBatchOutputConfig.id)
      }
      "save the data source" in {
        testDF.createOrReplaceTempView(dataSourceBatchOutputConfig.id)
        spark.catalog.tableExists(dataSourceBatchOutputConfig.id) shouldBe true
        val df = spark.table(dataSourceBatchOutputConfig.id)
        df.schema shouldBe testDF.schema
        output.write()
        spark.catalog.dropTempView(dataSourceBatchOutputConfig.id)
      }
      "save the data source after applying data quality" in {
        testDF.createOrReplaceTempView(dataSourceBatchOutputConfig.id)
        spark.catalog.tableExists(dataSourceBatchOutputConfig.id) shouldBe true
        val df = spark.table(dataSourceBatchOutputConfig.id)
        df.schema shouldBe testDF.schema
        new DataSourceBatchOutput(dataSourceBatchOutputConfig.copy(
          options = Some(Map("path" -> Files.createTempDirectory("hoolok_data_source_batch_output_").toUri.toString)),
          dq = Some(successDataQualityConfig)
        )).write()
        spark.catalog.dropTempView(dataSourceBatchOutputConfig.id)
      }
    }
  }

  s"a ${classOf[DataSourceStreamOutput].getSimpleName}" when {
    val output = new DataSourceStreamOutput(dataSourceStreamOutputConfig.copy(
      options = Some(Map("path" -> Files.createTempDirectory("hoolok_data_source_stream_output_").toUri.toString,
        "gracefulShutdownPath" -> Files.createTempDirectory("hoolok_graceful_").toUri.toString,
        "gracefulShutdownFileNotCreated" -> "false",
        "gracefulShutdownDelay" -> "30")
      )))
    val streamDF = MemoryStream[String].toDF()

    "initializing" should {
      "set the properties" in {
        output.gracefulShutdownPath.isDefined shouldBe true
        output.gracefulShutdownDelay shouldBe 30
        output.gracefulShutdownFileNotCreated shouldBe false
      }
      "have default properties" in {
        val output = new DataSourceStreamOutput(dataSourceStreamOutputConfig.copy(options = None))
        output.gracefulShutdownPath.isDefined shouldBe false
        output.gracefulShutdownDelay shouldBe 10
        output.gracefulShutdownFileNotCreated shouldBe true
      }
    }

    "writing" should {
      "fail if cannot read data" in {
        assertThrows[AnalysisException] {
          new DataSourceStreamOutput(dataSourceStreamOutputConfig.copy(schema = None)).write()
        }
      }
      "fail if the schemas cannot be matched" in {
        val output = new DataSourceStreamOutput(dataSourceStreamOutputConfig.copy(
          options = Some(Map("path" -> Files.createTempDirectory("hoolok_data_source_stream_output_").toUri.toString)),
          schema = Some("schema-test")
        ))
        streamDF.createOrReplaceTempView(dataSourceStreamOutputConfig.id)
        spark.catalog.tableExists(dataSourceStreamOutputConfig.id) shouldBe true
        val df = spark.table(dataSourceStreamOutputConfig.id)
        df.schema shouldBe streamDF.schema
        assertThrows[SchemaValidationException] {
          output.write()
        }
        spark.catalog.dropTempView(dataSourceStreamOutputConfig.id)
      }
      "save the data source" in {
        streamDF.createOrReplaceTempView(dataSourceStreamOutputConfig.id)
        spark.catalog.tableExists(dataSourceStreamOutputConfig.id) shouldBe true
        val df = spark.table(dataSourceStreamOutputConfig.id)
        df.schema shouldBe streamDF.schema
        output.write()
        spark.catalog.dropTempView(dataSourceStreamOutputConfig.id)
      }
    }
  }

}
