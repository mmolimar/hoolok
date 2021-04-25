package com.github.mmolimar.hoolok

import com.github.mmolimar.hoolok.common.Errors.{HoolokSuccess, UnknownHoolokError, YamlFileError}
import com.github.mmolimar.hoolok.common.{InvalidYamlFileException, MissingConfigFileException}

import java.io.FileNotFoundException
import java.nio.file.Files
import scala.reflect.io.File

class JobRunnerTest extends HoolokSparkTestHarness {

  s"a ${classOf[JobRunner].getSimpleName}" when {
    "validating the input" should {
      "throw an exception if the args are invalid" in {
        assertThrows[MissingConfigFileException] {
          JobRunner.validateArgs(Array.empty)
        }
        assertThrows[MissingConfigFileException] {
          JobRunner.validateArgs(Array("a", "b"))
        }
      }
    }
    "parsing the YAML config file" should {
      "fail if the file is invalid" in {
        assertThrows[FileNotFoundException] {
          JobRunner.parseConfigFile(s"${System.getProperty("java.io.tmpdir")}/doesnotexist")
        }
        assertThrows[InvalidYamlFileException] {
          JobRunner.parseConfigFile(Files.createTempFile("hoolok_config_", ".yml").toFile.getAbsolutePath)
        }
      }
      "print the banner" in {
        println(JobRunner.banner())
      }
      "map the file to a Hoolok config object" in {
        val configFile = File.makeTemp("hoolok_config_")
        File(configFile).writeAll(yamlConfig())
        val config = JobRunner.parseConfigFile(configFile.jfile.getAbsolutePath)
        config.app.name shouldBe "Test"
        config.app.sparkConf.isDefined shouldBe true
        config.app.sparkConf.get.size shouldBe 1
        config.schemas.isDefined shouldBe true
        config.schemas.get.size shouldBe 1
        config.inputs.size shouldBe 1
        config.steps.isDefined shouldBe true
        config.steps.get.size shouldBe 1
        config.outputs.size shouldBe 1
      }
    }
    "executing a job" should {
      "return an error as exit code if there are no inputs or outputs" in {
        val configFile = File.makeTemp("hoolok_config_")
        File(configFile).writeAll(yamlConfig())
        val config = JobRunner.parseConfigFile(configFile.jfile.getAbsolutePath)
        JobRunner.executeJob(config.copy(inputs = List.empty)) shouldBe YamlFileError.code
        JobRunner.executeJob(config.copy(outputs = List.empty)) shouldBe YamlFileError.code
      }
      "return an error as exit code if the input cannot be read" in {
        val configFile = File.makeTemp("hoolok_config_")
        File(configFile).writeAll(yamlConfig())
        val config = JobRunner.parseConfigFile(configFile.jfile.getAbsolutePath)
        val exitCode = JobRunner.executeJob(config)
        exitCode shouldBe UnknownHoolokError.code
      }
      "return success as exit code" in {
        val configFile = File.makeTemp("hoolok_config_")
        val inputDir = Files.createTempDirectory("hoolok_input_").toFile.getAbsolutePath
        val outputDir = Files.createTempDirectory("hoolok_output_").toFile.getAbsolutePath
        File(configFile).writeAll(yamlConfig(inputDir, outputDir))
        val config = JobRunner.parseConfigFile(configFile.jfile.getAbsolutePath)
        val exitCode = JobRunner.executeJob(config)
        exitCode shouldBe HoolokSuccess.code
      }
    }
  }

  def yamlConfig(inputPath: String = "file:///path/to/input", outputPath: String = "file:///path/to/output"): String =
    s"""
       |app:
       |  name: Test
       |  sparkConf:
       |    spark.master: local
       |schemas:
       |  - id: schema-test
       |    kind: inline
       |    format: spark-ddl
       |    options:
       |      value: key STRING, value STRING
       |inputs:
       |  - id: sample_datasource
       |    format: csv
       |    kind: batch
       |    schema: schema-test
       |    subtype: data-source
       |    options:
       |      path: $inputPath
       |steps:
       |  - id: sample_datasource
       |    kind: show
       |    options:
       |      numRows: "100"
       |outputs:
       |  - id: sample_datasource
       |    format: csv
       |    mode: overwrite
       |    kind: batch
       |    subtype: data-source
       |    options:
       |      path: $outputPath
       |      header: "true"
       |""".stripMargin
}
