package com.github.mmolimar.hoolok.schemas

import com.github.mmolimar.hoolok.HoolokSparkTestHarness
import com.github.mmolimar.hoolok.common.{InvalidSchemaConfigException, SchemaReadException}
import com.sun.net.httpserver.{HttpExchange, HttpHandler}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import java.net.ConnectException
import scala.collection.JavaConverters._

class SchemasTest extends HoolokSparkTestHarness {

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

  s"a ${classOf[InlineSchema].getSimpleName}" when {
    val schema = new InlineSchema(inlineSchemaConfig)

    "initializing" should {
      "fail if there are no options" in {
        assertThrows[InvalidSchemaConfigException] {
          new InlineSchema(inlineSchemaConfig.copy(options = None))
        }
      }
      "set the schema content" in {
        schema.content shouldBe "col1 INT, col2 STRING, col3 STRING, col4 BINARY, col5 BINARY, col6 BOOLEAN"
      }
    }
    "registering" should {
      "fail if the format is invalid" in {
        assertThrows[InvalidSchemaConfigException] {
          new InlineSchema(inlineSchemaConfig.copy(format = "invalid")).register()
        }
      }
      "cache the schema in the schema manager" in {
        SchemaManager.getSchema("inline-schema") shouldBe None
        schema.register()
        SchemaManager.getSchema("inline-schema").isDefined shouldBe true
        val inlineSchema = SchemaManager.getSchema("inline-schema").get
        inlineSchema.fields.length shouldBe 6
      }
    }
  }

  s"a ${classOf[UriSchema].getSimpleName}" when {
    val schema = new UriSchema(uriSchemaConfig)
    val path = getClass.getResource(schema.schemaPath.toString).toURI.toString

    "initializing" should {
      "fail if there are no options" in {
        assertThrows[InvalidSchemaConfigException] {
          new UriSchema(uriSchemaConfig.copy(options = None))
        }
      }
    }
    "registering" should {
      "fail if the format is invalid" in {
        assertThrows[InvalidSchemaConfigException] {
          new UriSchema(uriSchemaConfig.copy(format = "invalid")).register()
        }
      }
      "fail if the schema does not exist" in {
        assertThrows[SchemaReadException] {
          new UriSchema(uriSchemaConfig.copy(options = Some(Map("path" -> s"${path}_notexist")))).register()
        }
      }
      "cache the schema from the classpath in the schema manager" in {
        SchemaManager.getSchema("uri-schema1") shouldBe None
        schema.register()
        SchemaManager.getSchema("uri-schema1").isDefined shouldBe true
        val uriSchema = SchemaManager.getSchema("uri-schema1").get
        uriSchema.fields.length shouldBe 6
      }
      "cache the schema from the file system in the schema manager" in {
        val schema = new UriSchema(uriSchemaConfig.copy(id = "uri-schema2", options = Some(Map("path" -> path))))
        SchemaManager.getSchema("uri-schema2") shouldBe None
        schema.register()
        SchemaManager.getSchema("uri-schema2").isDefined shouldBe true
        val uriSchema = SchemaManager.getSchema("uri-schema2").get
        uriSchema.fields.length shouldBe 6
      }
      "cache the schema from an HTTP server" in {
        val schema = new UriSchema(uriSchemaConfig.copy(id = "uri-schema3",
          options = Some(Map("path" -> "http://localhost:47774/schemas/uri-schema3.json")))
        )
        SchemaManager.getSchema("uri-schema3") shouldBe None
        schema.register()
        SchemaManager.getSchema("uri-schema3").isDefined shouldBe true
        val uriSchema = SchemaManager.getSchema("uri-schema3").get
        uriSchema.fields.length shouldBe 6
      }
    }
  }

  s"a ${classOf[SchemaRegistrySchema].getSimpleName}" when {
    val schema = new SchemaRegistrySchema(schemaRegistrySchemaConfig)

    "initializing" should {
      "fail if there options are not valid" in {
        assertThrows[InvalidSchemaConfigException] {
          new SchemaRegistrySchema(schemaRegistrySchemaConfig.copy(options = None))
        }
        assertThrows[InvalidSchemaConfigException] {
          new SchemaRegistrySchema(schemaRegistrySchemaConfig
            .copy(options = Some(Map("schemaRegistryUrls" -> "http://localhost:1234"))))
        }
      }
      "set the schema subject" in {
        schema.schemaSubject.get shouldBe "test-subject"
      }
    }
    "registering" should {
      "fail if the format is invalid" in {
        assertThrows[InvalidSchemaConfigException] {
          new SchemaRegistrySchema(schemaRegistrySchemaConfig.copy(format = "invalid")).register()
        }
      }
      "fail due to a connection error" in {
        assertThrows[ConnectException] {
          try {
            new SchemaRegistrySchema(schemaRegistrySchemaConfig.copy(
              options = Some(Map("schemaRegistryUrls" -> "http://localhost:65432", "subject" -> "test-subject"))
            )).register()
          } catch {
            case t: SchemaReadException => throw t.getCause
          }
        }
      }
      "cache the schema in the schema manager by subject" in {
        SchemaManager.getSchema("schema-registry-schema1") shouldBe None
        schema.register()
        SchemaManager.getSchema("schema-registry-schema1").isDefined shouldBe true
        val inlineSchema = SchemaManager.getSchema("schema-registry-schema1").get
        inlineSchema.fields.length shouldBe 6
      }
      "cache the schema in the schema manager by id" in {
        val schema = new SchemaRegistrySchema(schemaRegistrySchemaConfig.copy(
          id = "schema-registry-schema2",
          options = Some(Map("schemaRegistryUrls" -> "http://localhost:47774", "id" -> "1"))
        ))
        SchemaManager.getSchema("schema-registry-schema2") shouldBe None
        schema.register()
        SchemaManager.getSchema("schema-registry-schema2").isDefined shouldBe true
        val inlineSchema = SchemaManager.getSchema("schema-registry-schema2").get
        inlineSchema.fields.length shouldBe 6
      }
      "cache the schema in the schema manager by subject and id" in {
        val schema = new SchemaRegistrySchema(schemaRegistrySchemaConfig.copy(
          id = "schema-registry-schema3",
          options = Some(Map("schemaRegistryUrls" -> "http://localhost:47774", "subject" -> "test-subject", "id" -> "1"))
        ))
        SchemaManager.getSchema("schema-registry-schema3") shouldBe None
        schema.register()
        SchemaManager.getSchema("schema-registry-schema3").isDefined shouldBe true
        val inlineSchema = SchemaManager.getSchema("schema-registry-schema3").get
        inlineSchema.fields.length shouldBe 6
      }
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    MockHttpServer.start()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    MockHttpServer.stop()
  }

}

// scalastyle:off
private object MockHttpServer {

  import com.sun.net.httpserver.HttpServer

  import java.net.InetSocketAddress

  val port: Int = 47774
  private lazy val server: HttpServer = {
    val schemaContent = "\"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"topLevelRecord\\\"," +
      "\\\"fields\\\":[{\\\"name\\\":\\\"col1\\\",\\\"type\\\":\\\"string\\\"}," +
      "{\\\"name\\\":\\\"col2\\\",\\\"type\\\":\\\"string\\\"},{\\\"name\\\":\\\"col3\\\"," +
      "\\\"type\\\":\\\"string\\\"},{\\\"name\\\":\\\"col4\\\",\\\"type\\\":\\\"string\\\"}," +
      "{\\\"name\\\":\\\"col5\\\",\\\"type\\\":\\\"string\\\"},{\\\"name\\\":\\\"col6\\\"" +
      ",\\\"type\\\":\\\"boolean\\\"}]}\""
    val srv = HttpServer.create(new InetSocketAddress("localhost", port), 0)
    srv.createContext("/schemas/uri-schema3.json", new HttpHandler() {
      override def handle(exchange: HttpExchange): Unit = {
        if ("GET" != exchange.getRequestMethod) {
          throw new RuntimeException("Invalid method.")
        }
        val content = "{\"definitions\":{},\"$schema\":\"http://json-schema.org/draft-07/schema#\"," +
          "\"$id\":\"http://example.com/root.json\",\"type\":\"object\",\"title\":\"Test Schema\"," +
          "\"properties\":{\"col1\":{\"$id\":\"#/properties/col1\",\"type\":\"integer\"},\"col2\":" +
          "{\"$id\":\"#/properties/col2\",\"type\":\"string\"},\"col3\":{\"$id\":\"#/properties/col3\"," +
          "\"type\":\"string\"},\"col4\":{\"$id\":\"#/properties/col4\",\"type\":\"string\"},\"col5\":" +
          "{\"$id\":\"#/properties/col5\",\"type\":\"string\"},\"col6\":{\"$id\":\"#/properties/col6\"," +
          "\"type\":\"boolean\"}}}"
        exchange.sendResponseHeaders(200, content.length())
        val os = exchange.getResponseBody
        os.write(content.getBytes())
        os.flush()
        os.close()
      }
    })
    srv.createContext("/subjects/test-subject/versions/latest", new HttpHandler() {
      override def handle(exchange: HttpExchange): Unit = {
        if ("GET" != exchange.getRequestMethod) {
          throw new RuntimeException("Invalid method.")
        }
        val content =
          s"""
             |{
             |  "subject": "test-subject",
             |  "version": 1,
             |  "id": 1,
             |  "schema": $schemaContent
             |}
             |""".stripMargin
        exchange.sendResponseHeaders(200, content.length())
        val os = exchange.getResponseBody
        os.write(content.getBytes())
        os.flush()
        os.close()
      }
    })
    srv.createContext("/schemas/ids/1", new HttpHandler() {
      override def handle(exchange: HttpExchange): Unit = {
        if ("GET" != exchange.getRequestMethod) {
          throw new RuntimeException("Invalid method.")
        }
        val content =
          s"""
             |{
             |  "schema": $schemaContent
             |}
             |""".stripMargin
        exchange.sendResponseHeaders(200, content.length())
        val os = exchange.getResponseBody
        os.write(content.getBytes())
        os.flush()
        os.close()
      }
    })
    srv
  }

  def start(): Unit = {
    server.start()
  }

  def stop(): Unit = {
    server.stop(1)
  }

}
