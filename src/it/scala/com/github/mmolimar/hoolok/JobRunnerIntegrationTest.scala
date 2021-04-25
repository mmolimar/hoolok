package com.github.mmolimar.hoolok

import com.github.mmolimar.hoolok.common.Errors.HoolokSuccess
import com.github.mmolimar.hoolok.common.Utils._
import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.schemaregistry.rest.{SchemaRegistryConfig, SchemaRegistryRestApplication}
import io.confluent.rest.RestConfig
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.avro.{Schema => AvroSchema}

import java.io.{File => JFile}
import java.net.URI
import java.nio.file.Files
import java.util.Properties
import scala.io.Source

class JobRunnerIntegrationTest extends HoolokSparkTestHarness with EmbeddedKafka {

  lazy implicit val kafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig()

  s"a ${classOf[JobRunnerIntegrationTest].getSimpleName}" when {
    "executing a job" should {
      "be executed successfully when running a batch job" in {
        createProps()
        val configFile = getClass.getResource("/config/batch.yaml").getFile
        val config = JobRunner.parseConfigFile(configFile)
        JobRunner.executeJob(config) shouldBe HoolokSuccess.code
      }
      "be executed successfully when running a streaming job" in {
        val props = createProps()
        val configFile = getClass.getResource("/config/streaming.yaml").getFile
        val config = JobRunner.parseConfigFile(configFile)
        stopEventuallyStreams(props.gracefulShutdownPath)
        JobRunner.executeJob(config) shouldBe HoolokSuccess.code
      }
      "be executed successfully when running a batch-streaming job" in {
        withRunningKafka {
          val props = createProps()
          val schemaRegistryPort = 52025
          withSchemaRegistry(schemaRegistryPort, kafkaConfig.kafkaPort) {
            val mapCapacity = 10
            val inputSchemaContent = closer(
              Source.fromFile(new JFile(getClass.getResource("/schemas/input-schema.avsc").toURI))
            )(_.mkString)
            val outputSchemaContent = closer(
              Source.fromFile(new JFile(getClass.getResource("/schemas/output-schema.avsc").toURI))
            )(_.mkString)
            val kafkaSchemaContent = closer(
              Source.fromFile(new JFile(getClass.getResource("/schemas/kafka-schema.avsc").toURI))
            )(_.mkString)

            val baseUrl = s"http://localhost:$schemaRegistryPort"
            val client = new CachedSchemaRegistryClient(baseUrl, mapCapacity)
            client.setMode("IMPORT")
            client.register("schema_input_bs", new AvroSchema.Parser().parse(inputSchemaContent), 1, 1)
            client.register("schema_output_bs", new AvroSchema.Parser().parse(outputSchemaContent), 1, 2)
            client.register("schema_kafka", new AvroSchema.Parser().parse(kafkaSchemaContent), 1, 3)

            System.setProperty("SCHEMA_REGISTRY_URL", baseUrl)
            System.setProperty("KAFKA_BOOTSTRAP_SERVERS", s"localhost:${kafkaConfig.kafkaPort}")
            System.setProperty("KAFKA_TOPIC", "courses_ext_avro")
            val configFile = getClass.getResource("/config/batch-and-streaming.yaml").getFile
            val config = JobRunner.parseConfigFile(configFile)

            printConf(config)
            stopEventuallyStreams(props.gracefulShutdownPath)
            JobRunner.executeJob(config) shouldBe HoolokSuccess.code
          }
        }
      }
    }
  }

  private case class Props(inputPath: String, outputPath: String, gracefulShutdownPath: String)

  private def createProps(): Props = {
    val inputPath = new JFile(getClass.getResource("/data/courses.csv").toURI).getParentFile.toURI.toString
    val outputPath = {
      Files.createTempDirectory("hoolok_output_").toUri.toString + "hoolok" +
        JFile.separator + "output"
    }
    val gracefulShutdownPath = Files.createTempDirectory("hoolok_graceful_").toFile.toURI.toString
    System.setProperty("INPUT_PATH", inputPath)
    System.setProperty("OUTPUT_PATH", outputPath)
    System.setProperty("GRACEFUL_SHUTDOWN_PATH", gracefulShutdownPath)

    Props(inputPath, outputPath, gracefulShutdownPath)
  }

  // scalastyle:off
  private def stopEventuallyStreams(gracefulShutdownPath: String, waitTime: Long = 40000L): Unit = {
    new Thread() {
      override def run(): Unit = {
        Thread.sleep(waitTime)
        new JFile(new URI(gracefulShutdownPath).getPath).listFiles().forall(_.delete())
      }
    }.start()
  }

  private def printConf(config: HoolokConfig): Unit = {
    import io.circe.generic.auto._
    import io.circe.syntax._
    import io.circe.yaml
    val content = yaml.Printer(preserveOrder = true, dropNullKeys = true).pretty(config.asJson)
    println(s"Config file content: \n$content")
  }

  private def withSchemaRegistry(schemaRegistryPort: Int, kafkaPort: Int)(body: => Unit): Unit = {
    val schemaRegistry = {
      val props = new Properties()
      props.put(RestConfig.LISTENERS_CONFIG, s"http://localhost:$schemaRegistryPort")
      props.put(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, s"localhost:$kafkaPort")
      props.setProperty(SchemaRegistryConfig.KAFKASTORE_CONNECTION_URL_CONFIG, s"localhost:${kafkaConfig.zooKeeperPort}")
      props.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, SchemaRegistryConfig.DEFAULT_KAFKASTORE_TOPIC);
      props.put(SchemaRegistryConfig.COMPATIBILITY_CONFIG, AvroCompatibilityLevel.NONE.name);
      props.put(SchemaRegistryConfig.MASTER_ELIGIBILITY, "true");
      props.put(SchemaRegistryConfig.MODE_MUTABILITY, "true");
      new SchemaRegistryRestApplication(props).createServer()
    }
    try {
      schemaRegistry.start()
      body
    } finally {
      schemaRegistry.stop()
    }
  }

}
