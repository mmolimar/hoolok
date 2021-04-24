package com.github.mmolimar.hoolok.steps

import com.github.mmolimar.hoolok._
import com.github.mmolimar.hoolok.common.{DataQualityValidationException, InvalidStepConfigException}
import com.github.mmolimar.hoolok.schemas._
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.storage.StorageLevel

import java.time.temporal.ChronoUnit
import java.time.{Duration, LocalDateTime, Period}
import java.util.Calendar

// scalastyle:off
class StepsTest extends HoolokSparkTestHarness {

  import spark.implicits._

  implicit val sqlContext: SQLContext = spark.sqlContext

  val rows = Seq(
    (1, """{"field1":"value1"}""", """{"field2":"value2"}""", None.orNull, None.orNull, true),
    (2, """{"field1":"value1"}""", """{"field2":"value2"}""", None.orNull, None.orNull, false)
  )
  val testDF: DataFrame = rows.toDF("col1", "col2", "col3", "col4", "col5", "col6")
  registerSchemas()

  s"a ${classOf[ShowStep].getSimpleName}" when {
    val step = new ShowStep(showConfig)

    "initializing" should {
      "have default values" in {
        step.numRows shouldBe 20
        step.truncate shouldBe true
      }
      "set config variables" in {
        val step = new ShowStep(showConfig.copy(options = Some(Map("numRows" -> "50", "truncate" -> "false"))))
        step.numRows shouldBe 50
        step.truncate shouldBe false
      }
    }

    "processing" should {
      "show a dataframe" in {
        testDF.createOrReplaceTempView(showConfig.id)
        step.process()
      }
      "not be executed if the dataframe is a stream" in {
        val step = new ShowStep(showConfig.copy(id = "test_stream"))
        testDF.createOrReplaceTempView(showConfig.id)
        val streamDF = MemoryStream[String].toDF()
        streamDF.createOrReplaceTempView(step.config.id)
        step.process()
        spark.table(step.config.id).isStreaming shouldBe true
      }
    }
  }

  s"a ${classOf[CacheStep].getSimpleName}" when {
    val step = new CacheStep(cacheConfig)

    "initializing" should {
      "have default values" in {
        step.storageLevel shouldBe StorageLevel.MEMORY_AND_DISK
      }
      "set config variables" in {
        val step = new CacheStep(cacheConfig.copy(options = Some(Map("storageLevel" -> "off_heap"))))
        step.storageLevel shouldBe StorageLevel.OFF_HEAP
      }
    }

    "processing" should {
      "cache a dataframe" in {
        testDF.createOrReplaceTempView(cacheConfig.id)
        spark.catalog.isCached("test") shouldBe false
        step.process()
        spark.catalog.isCached("test") shouldBe true
        testDF.unpersist(true)
      }
      "not be executed if the dataframe is a stream" in {
        val step = new CacheStep(cacheConfig.copy(id = "test_stream"))
        val streamDF = MemoryStream[String].toDF()
        streamDF.createOrReplaceTempView(step.config.id)
        spark.catalog.isCached("test_stream") shouldBe false
        step.process()
        spark.catalog.isCached("test_stream") shouldBe false
        spark.table(step.config.id).isStreaming shouldBe true
      }
    }
  }

  s"a ${classOf[UnpersistStep].getSimpleName}" when {
    val step = new UnpersistStep(unpersistConfig)

    "initializing" should {
      "have default values" in {
        step.blocking shouldBe false
      }
      "set config variables" in {
        val step = new UnpersistStep(unpersistConfig.copy(options = Some(Map("blocking" -> "true"))))
        step.blocking shouldBe true
      }
    }

    "processing" should {
      "cache a dataframe" in {
        testDF.createOrReplaceTempView(unpersistConfig.id)
        spark.catalog.isCached("test") shouldBe false
        testDF.persist()
        spark.catalog.isCached("test") shouldBe true
        step.process()
        spark.catalog.isCached("test") shouldBe false
      }
      "not be executed if the dataframe is a stream" in {
        val step = new UnpersistStep(unpersistConfig.copy(id = "test_stream"))
        val streamDF = MemoryStream[String].toDF()
        streamDF.createOrReplaceTempView(step.config.id)
        spark.catalog.isCached("test_stream") shouldBe false
        step.process()
        spark.catalog.isCached("test_stream") shouldBe false
        spark.table(step.config.id).isStreaming shouldBe true
      }
    }
  }

  s"a ${classOf[CheckpointStep].getSimpleName}" when {
    val step = new CheckpointStep(checkpointConfig)

    "initializing" should {
      "have default values" in {
        step.eager shouldBe true
        step.reliableCheckpoint shouldBe false
      }
      "set config variables" in {
        val step = new CheckpointStep(checkpointConfig.copy(options = Some(Map("eager" -> "false", "reliableCheckpoint" -> "true"))))
        step.eager shouldBe false
        step.reliableCheckpoint shouldBe true
      }
    }

    "processing" should {
      "create a local checkpoint" in {
        val step = new CheckpointStep(checkpointConfig.copy(options = Some(Map("reliableCheckpoint" -> "false"))))
        step.process()
      }
      "create a reliable checkpoint" in {
        val step = new CheckpointStep(checkpointConfig.copy(options = Some(Map("reliableCheckpoint" -> "true"))))
        step.process()
      }
      "not be executed if the dataframe is a stream" in {
        val step = new CheckpointStep(checkpointConfig.copy(id = "test_stream"))
        val streamDF = MemoryStream[String].toDF()
        streamDF.createOrReplaceTempView(step.config.id)
        step.process()
        spark.table(step.config.id).isStreaming shouldBe true
      }
    }
  }

  s"a ${classOf[FromJsonStep].getSimpleName}" when {
    val step = new FromJsonStep(fromJsonConfig)
    testDF.createOrReplaceTempView(fromJsonConfig.id)

    "initializing" should {
      "fail if there are not enough options" in {
        assertThrows[InvalidStepConfigException] {
          new FromJsonStep(fromJsonConfig.copy(options = None))
        }
        assertThrows[InvalidStepConfigException] {
          new FromJsonStep(fromJsonConfig.copy(options = Some(Map("dataframe" -> "test"))))
        }
        assertThrows[InvalidStepConfigException] {
          new FromJsonStep(fromJsonConfig.copy(options = Some(Map("dataframe" -> "test", "columns" -> "col2,col3"))))
        }
        assertThrows[InvalidStepConfigException] {
          new FromJsonStep(fromJsonConfig.copy(options = Some(Map(
            "dataframe" -> "test", "columns" -> "col2,col3", "alias" -> "alias_col2,alias_col3"))))
        }
      }
      "fail if the number of columns do not match with the number of aliases" in {
        assertThrows[InvalidStepConfigException] {
          new FromJsonStep(fromJsonConfig.copy(options = Some(fromJsonConfig.options.get + ("alias" -> "alias_col2"))))
        }
        assertThrows[InvalidStepConfigException] {
          new FromJsonStep(fromJsonConfig.copy(options = Some(fromJsonConfig.options.get + ("columns" -> "col2"))))
        }
      }
    }

    "processing" should {
      "transform the JSON columns" in {
        assertThrows[InvalidStepConfigException] {
          new FromJsonStep(fromJsonConfig.copy(options = Some(fromJsonConfig.options.get + ("schema" -> "notexist")))).process()
        }
        spark.catalog.dropTempView("test_json")
        spark.catalog.tableExists("test_json") shouldBe false
        step.process()
        spark.catalog.tableExists("test_json") shouldBe true
        val jsonDF = spark.table("test_json")
        val fields = jsonDF.schema.fields
        fields.length shouldBe 2
        fields.map(_.name).contains("field1") shouldBe true
        fields.map(_.name).contains("field2") shouldBe true
        fields.map(_.dataType).forall(_ == StringType) shouldBe true
        spark.catalog.dropTempView("test_json")
      }
    }
  }

  s"a ${classOf[FromAvroStep].getSimpleName}" when {
    val step = new FromAvroStep(fromAvroConfig)
    testDF.createOrReplaceTempView(fromAvroConfig.id)

    "initializing" should {
      "fail if there are not enough options" in {
        assertThrows[InvalidStepConfigException] {
          new FromJsonStep(fromAvroConfig.copy(options = None))
        }
        assertThrows[InvalidStepConfigException] {
          new FromJsonStep(fromAvroConfig.copy(options = Some(Map("dataframe" -> "test"))))
        }
        assertThrows[InvalidStepConfigException] {
          new FromJsonStep(fromAvroConfig.copy(options = Some(Map("dataframe" -> "test", "columns" -> "col4,col5"))))
        }
        assertThrows[InvalidStepConfigException] {
          new FromJsonStep(fromAvroConfig.copy(options = Some(Map(
            "dataframe" -> "test", "columns" -> "col4,col5", "alias" -> "alias_col4,alias_col5"))))
        }
      }
      "fail if the number of columns do not match with the number of aliases" in {
        assertThrows[InvalidStepConfigException] {
          new FromJsonStep(fromAvroConfig.copy(options = Some(fromAvroConfig.options.get + ("alias" -> "alias_col4"))))
        }
        assertThrows[InvalidStepConfigException] {
          new FromJsonStep(fromAvroConfig.copy(options = Some(fromAvroConfig.options.get + ("columns" -> "col4"))))
        }
      }
    }

    "processing" should {
      "transform the AVRO columns" in {
        assertThrows[InvalidStepConfigException] {
          new FromJsonStep(fromAvroConfig.copy(options = Some(fromAvroConfig.options.get + ("schema" -> "notexist")))).process()
        }
        spark.catalog.dropTempView("test_avro")
        spark.catalog.tableExists("test_avro") shouldBe false
        step.process()
        spark.catalog.tableExists("test_avro") shouldBe true
        val jsonDF = spark.table("test_avro")
        val fields = jsonDF.schema.fields
        fields.length shouldBe 2
        fields.map(_.name).contains("field1") shouldBe true
        fields.map(_.name).contains("field2") shouldBe true
        fields.map(_.dataType).forall(_ == StringType) shouldBe true
        spark.catalog.dropTempView("test_avro")
      }
    }
  }

  s"a ${classOf[ToAvroStep].getSimpleName}" when {
    val step = new ToAvroStep(toAvroConfig)
    testDF.createOrReplaceTempView(toAvroConfig.id)

    "initializing" should {
      "fail if there are not enough options" in {
        assertThrows[InvalidStepConfigException] {
          new FromJsonStep(toAvroConfig.copy(options = None))
        }
        assertThrows[InvalidStepConfigException] {
          new FromJsonStep(toAvroConfig.copy(options = Some(Map("dataframe" -> "test"))))
        }
        assertThrows[InvalidStepConfigException] {
          new FromJsonStep(toAvroConfig.copy(options = Some(Map("dataframe" -> "test", "columns" -> "col2,col3"))))
        }
        assertThrows[InvalidStepConfigException] {
          new FromJsonStep(toAvroConfig.copy(options = Some(Map(
            "dataframe" -> "test", "columns" -> "col2,col3", "alias" -> "alias_col2,alias_col3"))))
        }
      }
      "fail if the number of columns do not match with the number of aliases" in {
        assertThrows[InvalidStepConfigException] {
          new FromJsonStep(toAvroConfig.copy(options = Some(toAvroConfig.options.get + ("alias" -> "alias_col2"))))
        }
        assertThrows[InvalidStepConfigException] {
          new FromJsonStep(toAvroConfig.copy(options = Some(toAvroConfig.options.get + ("columns" -> "col2"))))
        }
      }
    }

    "processing" should {
      "transform columns to AVRO" in {
        assertThrows[InvalidStepConfigException] {
          new FromJsonStep(toAvroConfig.copy(options = Some(toAvroConfig.options.get + ("schema" -> "notexist")))).process()
        }
        spark.catalog.dropTempView("test_avro")
        spark.catalog.tableExists("test_avro") shouldBe false
        step.process()
        spark.catalog.tableExists("test_avro") shouldBe true
        val jsonDF = spark.table("test_avro")
        val fields = jsonDF.schema.fields
        fields.length shouldBe 2
        fields.map(_.name).contains("alias_col2") shouldBe true
        fields.map(_.name).contains("alias_col3") shouldBe true
        fields.map(_.dataType).forall(_ == BinaryType) shouldBe true
        spark.catalog.dropTempView("test_avro")
      }
    }
  }

  s"a ${classOf[SqlStep].getSimpleName}" when {
    val step = new SqlStep(sqlConfig)
    testDF.createOrReplaceTempView(sqlConfig.id)

    "initializing" should {
      "fail if there is no query specified" in {
        assertThrows[InvalidStepConfigException] {
          new SqlStep(sqlConfig.copy(options = None))
        }
      }
      "render the Jinja template" in {
        val jinjaSql =
          """{{ hoolok_utils:current_timestamp() }}
            |{{ hoolok_utils:current_year() }}
            |{{ hoolok_utils:current_month() }}
            |{{ hoolok_utils:current_day() }}
            |{{ hoolok_utils:current_day_of_week() }}
            |{{ hoolok_utils:current_day_of_year() }}
            |{{ hoolok_utils:current_hour() }}
            |{{ hoolok_utils:current_hour_of_day() }}
            |{{ hoolok_utils:current_minute() }}
            |{{ hoolok_utils:previous_year() }}
            |{{ hoolok_utils:previous_month() }}
            |{{ hoolok_utils:previous_day() }}
            |{{ hoolok_utils:previous_day_of_week() }}
            |{{ hoolok_utils:previous_day_of_year() }}
            |{{ hoolok_utils:previous_hour() }}
            |{{ hoolok_utils:previous_hour_of_day() }}
            |{{ hoolok_utils:previous_minute() }}
            |{{ hoolok_utils:date_add_days('2000-10-01', 'yyyy-MM-dd', 'yyyy-MM-dd', 2) }}
            |{{ hoolok_utils:date_add_weeks('2000-10-01', 'yyyy-MM-dd', 'yyyy-MM-dd', 2) }}
            |{{ hoolok_utils:date_add_months('2000-10-01', 'yyyy-MM-dd', 'yyyy-MM-dd', 2) }}
            |{{ hoolok_utils:date_add_years('2000-10-01', 'yyyy-MM-dd', 'yyyy-MM-dd', 2) }}
            |{{ hoolok_utils:last_day_current_month() }}
            |{{ hoolok_utils:last_day_of_month('2000-02-01', 'yyyy-MM-dd') }}
            |{{ hoolok_utils:to_lower('LOWER CASE') }}
            |{{ hoolok_utils:to_upper('upper case') }}
            |{{ hoolok_utils:capitalize('capitalize text') }}
            |""".stripMargin
        val step = new SqlStep(sqlConfig.copy(options = Some(Map("query" -> jinjaSql))))
        val rendered =
          s"""${LocalDateTime.now.getYear}
             |${LocalDateTime.now.getMonthValue}
             |${LocalDateTime.now.getDayOfMonth}
             |${LocalDateTime.now.getDayOfWeek.getValue}
             |${LocalDateTime.now.getDayOfYear}
             |${LocalDateTime.now.getHour}
             |${LocalDateTime.now.getHour % 12}
             |${LocalDateTime.now.getMinute}
             |${LocalDateTime.now.minus(Period.ofYears(1)).getYear}
             |${LocalDateTime.now.minus(Period.ofMonths(1)).getMonthValue}
             |${LocalDateTime.now.minus(Period.ofDays(1)).getDayOfMonth}
             |${LocalDateTime.now.minus(Period.ofDays(1)).getDayOfWeek.getValue}
             |${LocalDateTime.now.minus(Period.ofDays(1)).getDayOfYear}
             |${LocalDateTime.now.minus(Duration.of(1, ChronoUnit.HOURS)).getHour}
             |${LocalDateTime.now.minus(Duration.of(1, ChronoUnit.HOURS)).getHour % 12}
             |${LocalDateTime.now.minus(Duration.of(1, ChronoUnit.MINUTES)).getMinute}
             |2000-10-03
             |2000-10-15
             |2000-12-01
             |2002-10-01
             |${Calendar.getInstance.getActualMaximum(Calendar.DAY_OF_MONTH)}
             |29
             |lower case
             |UPPER CASE
             |Capitalize text""".stripMargin

        step.query.split("\n").tail.mkString("\n") shouldBe rendered
      }
    }

    "processing" should {
      "execute the query" in {
        spark.catalog.dropTempView("test_sql")
        spark.catalog.tableExists("test_sql") shouldBe false
        step.process()
        spark.catalog.tableExists("test_sql") shouldBe true
        val jsonDF = spark.table("test_sql")
        val fields = jsonDF.schema.fields
        fields.length shouldBe 3
        fields.foreach {
          case f: StructField if f.name == "col1" => f.dataType shouldBe IntegerType
          case f: StructField if f.name == "col2" => f.dataType shouldBe StringType
          case f: StructField if f.name == "col6" => f.dataType shouldBe BooleanType
        }
        spark.catalog.dropTempView("test_sql")
      }

      "fail if the data quality validation does not pass" in {
        val dq = HoolokDataQualityConfig(
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
        val step = new SqlStep(sqlConfig.copy(dq = Some(dq)))
        assertThrows[DataQualityValidationException] {
          step.process()
        }
      }
    }
  }

}
