package com.github.mmolimar.hoolok.inputs

import com.github.mmolimar.hoolok.HoolokSparkTestHarness
import com.github.mmolimar.hoolok.common.InvalidInputConfigException
import com.github.mmolimar.hoolok.schemas.registerSchemas
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SQLContext}

import java.nio.file.Files
import scala.collection.JavaConverters._

class InputsTest extends HoolokSparkTestHarness {

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

  s"a ${classOf[TableBatchInput].getSimpleName}" when {
    val input = new TableBatchInput(tableBatchInputConfig)
    val tableName = tableBatchInputConfig.options.get("tableName")

    "initializing" should {
      "fail if there are no options" in {
        assertThrows[InvalidInputConfigException] {
          new TableBatchInput(tableBatchInputConfig.copy(options = None))
        }
      }
      "set table name" in {
        input.tableName shouldBe "batch_table"
      }
    }
    "reading" should {
      "load the table" in {
        testDF.createOrReplaceTempView(tableName)
        spark.catalog.tableExists(tableBatchInputConfig.id) shouldBe false
        input.read()
        spark.catalog.tableExists(tableBatchInputConfig.id) shouldBe true
        val df = spark.table(tableBatchInputConfig.id)
        df.schema shouldBe testDF.schema
        spark.catalog.dropTempView(tableBatchInputConfig.id)
      }
    }
  }

  s"a ${classOf[TableStreamInput].getSimpleName}" when {
    val input = new TableStreamInput(tableStreamInputConfig)
    val tableName = tableStreamInputConfig.options.get("tableName")
    val streamDF = MemoryStream[String].toDF()

    "initializing" should {
      "fail if there are no options" in {
        assertThrows[InvalidInputConfigException] {
          new TableStreamInput(tableStreamInputConfig.copy(options = None))
        }
      }
      "set table name" in {
        input.tableName shouldBe "stream_table"
      }
    }
    "reading" should {
      "load the table" in {
        streamDF.createOrReplaceTempView(tableName)
        spark.catalog.tableExists(tableStreamInputConfig.id) shouldBe false
        input.read()
        spark.catalog.tableExists(tableStreamInputConfig.id) shouldBe true
        val df = spark.table(tableStreamInputConfig.id)
        df.schema.fields.length shouldBe 1
        df.schema.fields.head.dataType shouldBe StringType
        spark.catalog.dropTempView(tableStreamInputConfig.id)
      }
    }
  }

  s"a ${classOf[DataSourceBatchInput].getSimpleName}" when {
    "reading" should {
      "fail if cannot infer the schema" in {
        assertThrows[AnalysisException] {
          new DataSourceBatchInput(dataSourceBatchInputConfig.copy(schema = None)).read()
        }
      }
      "load the table" in {
        val tmpDir = Files.createTempDirectory("hoolok_batch_data_source_").toUri.toString
        testDF.write.mode("overwrite").csv(tmpDir)
        val config = dataSourceBatchInputConfig.copy(options = Some(Map("path" -> tmpDir)))
        val input = new DataSourceBatchInput(config)
        spark.catalog.tableExists(dataSourceBatchInputConfig.id) shouldBe false
        input.read()
        spark.catalog.tableExists(dataSourceBatchInputConfig.id) shouldBe true
        val df = spark.table(dataSourceBatchInputConfig.id)
        df.schema shouldBe testDF.schema
        spark.catalog.dropTempView(dataSourceBatchInputConfig.id)
      }
    }
  }

  s"a ${classOf[DataSourceStreamInput].getSimpleName}" when {
    "reading" should {
      "fail if cannot read data" in {
        assertThrows[IllegalArgumentException] {
          new DataSourceStreamInput(dataSourceStreamInputConfig.copy(schema = None)).read()
        }
      }
      "load the table" in {
        val tmpDir = Files.createTempDirectory("hoolok_batch_data_source_").toUri.toString
        testDF.write.mode("overwrite").csv(tmpDir)
        val config = dataSourceStreamInputConfig.copy(options = Some(Map("path" -> tmpDir)))
        val input = new DataSourceStreamInput(config)
        spark.catalog.tableExists(dataSourceStreamInputConfig.id) shouldBe false
        input.read()
        spark.catalog.tableExists(dataSourceStreamInputConfig.id) shouldBe true
        val df = spark.table(dataSourceStreamInputConfig.id)
        df.schema shouldBe testDF.schema
        spark.catalog.dropTempView(dataSourceStreamInputConfig.id)
      }
    }
  }

}
