package com.github.mmolimar.hoolok

import cats.syntax.either.catsSyntaxEither
import com.github.mmolimar.hoolok.common.Errors.{HoolokSuccess, UnknownHoolokError}
import com.github.mmolimar.hoolok.common.Implicits.SparkSessionBuilderOptions
import com.github.mmolimar.hoolok.common.{HoolokException, InvalidConfigException, MissingConfigFileException}
import com.github.mmolimar.hoolok.inputs.{Input, InputFactory}
import com.github.mmolimar.hoolok.outputs.{Output, OutputFactory}
import com.github.mmolimar.hoolok.schemas.{Schema, SchemaFactory}
import com.github.mmolimar.hoolok.steps.{Step, StepFactory}
import io.circe.generic.auto.exportDecoder
import io.circe.{Error, yaml}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import java.io.FileReader
import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

private[hoolok] class JobRunner(config: HoolokConfig) extends Logging {

  private implicit lazy val spark: SparkSession = initialize(config.app)

  def execute(): Unit = {
    val (schemas, inputs, steps, outputs) = validate(config)

    logInfo("Registering schemas...")
    schemas.foreach(_.register())

    logInfo("Reading inputs...")
    inputs.foreach(_.read())

    logInfo("Processing steps...")
    steps.foreach(_.process())

    logInfo("Writing outputs...")
    outputs.foreach(_.write())

    waitForStreams()
  }

  @tailrec
  private def waitForStreams(): Unit = {
    if (spark.streams.active.nonEmpty) {
      spark.streams.awaitAnyTermination()
      spark.streams.resetTerminated()
      waitForStreams()
    }
  }

  def stop(): Unit = spark.stop()

  private[hoolok] def validate(config: HoolokConfig): (List[Schema], List[Input], List[Step], List[Output]) = {
    if (config.inputs.isEmpty || config.outputs.isEmpty) {
      throw new InvalidConfigException("The YAML config file must have, at least one input and one output.")
    }
    logInfo("Validating schemas, inputs, steps and outputs config...")
    val schemas = config.schemas.getOrElse(List.empty).map(SchemaFactory(_))
    val inputs = config.inputs.map(InputFactory(_))
    val steps = config.steps.map(StepFactory(_))
    val outputs = config.outputs.map(OutputFactory(_))

    (schemas, inputs, steps, outputs)
  }

  private[hoolok] def initialize(appConfig: HoolokAppConfig): SparkSession = {
    logInfo(s"Initializing Spark Session for '${config.app.name}'.")
    val spark = SparkSession.builder()
      .appName(appConfig.name)
      .withHiveSupport(appConfig.enableHiveSupport)
      .withSparkConf(appConfig.sparkConf.getOrElse(Map.empty))
      .getOrCreate()

    appConfig.sparkContext.foreach { sc =>
      sc.archive.foreach(spark.sparkContext.addArchive)
      sc.description.foreach(spark.sparkContext.setJobDescription)
      sc.file.foreach(spark.sparkContext.addArchive)
      sc.jar.foreach(spark.sparkContext.addJar)
      sc.hadoopConfiguration.getOrElse(Map.empty).foreach {
        case (k, v) => spark.sparkContext.hadoopConfiguration.set(k, v)
      }
    }

    spark
  }

}

object JobRunner extends App with Logging {

  if (args.length != 1) {
    throw new MissingConfigFileException(s"There are ${args.length} arguments. Expected 1 with " +
      "the config file path.")
  }

  logInfo(
    """
      |______  __           ______     ______
      |___  / / /______________  /________  /__
      |__  /_/ /_  __ \  __ \_  /_  __ \_  //_/
      |_  __  / / /_/ / /_/ /  / / /_/ /  ,<       version %s
      |/_/ /_/  \____/\____//_/  \____//_/|_|
      |________________________________________
      |
      |"""
      .stripMargin.format(BuildInfo.version))

  logInfo(s"Parsing Hoolok YAML file located at: '${args.head}'")
  private val config = yaml.parser.parse(new FileReader(args.head))
    .leftMap(err => err: Error)
    .flatMap(_.as[HoolokConfig])
    .valueOr(err => throw new InvalidConfigException(
      message = s"Cannot parse YAML file. ${err.getMessage}",
      cause = err
    ))

  logInfo("Executing Spark job with this job config: \n" + config)
  val runner = new JobRunner(config)
  val exitCode = Try {
    runner.execute()
  } match {
    case Failure(he: HoolokException) =>
      logError(he.getMessage, he)
      he.error.code
    case Failure(t: Throwable) =>
      logError(s"Unexpected error: ${t.getMessage}", t)
      UnknownHoolokError.code
    case Success(_) => HoolokSuccess.code
  }
  runner.stop()

  sys.exit(exitCode)
}
