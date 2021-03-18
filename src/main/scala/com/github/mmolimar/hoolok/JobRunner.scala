package com.github.mmolimar.hoolok

import cats.syntax.either.catsSyntaxEither
import com.github.mmolimar.hoolok.errors.UnknownHoolokError
import com.github.mmolimar.hoolok.implicits.SparkSessionBuilderOptions
import io.circe.generic.auto.exportDecoder
import io.circe.{Error, yaml}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import java.io.FileReader

private[hoolok] class JobRunner(config: HoolokConfig) extends Logging {

  lazy implicit val spark: SparkSession = initialize(config.app)

  def execute(): Unit = {
    val (inputs, steps, outputs) = validate(config)

    logInfo("Reading inputs...")
    inputs.foreach(_.read())

    logInfo("Processing steps...")
    steps.foreach(_.process())

    logInfo("Writing outputs...")
    outputs.foreach(_.write())
  }

  private[hoolok] def validate(config: HoolokConfig): (List[Input], List[Step], List[Output]) = {
    if (config.inputs.isEmpty || config.outputs.isEmpty) {
      throw new InvalidConfigException("The YAML config file must have, at least one input and one output.")
    }
    logInfo("Validating inputs, steps and outputs config...")
    val inputs = config.inputs.map(Input(_))
    val steps = config.steps.map(Step(_))
    val outputs = config.outputs.map(Output(_))

    (inputs, steps, outputs)
  }

  private[hoolok] def initialize(appConfig: HoolokAppConfig): SparkSession = {
    logInfo(s"Initializing Spark Session for '${config.app.name}'.")
    val spark = SparkSession.builder()
      .appName(appConfig.name)
      .withSparkConf(appConfig.sparkConf.getOrElse(Map.empty))
      .getOrCreate()
    appConfig.description.foreach(desc => spark.sparkContext.setJobDescription(desc))

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
      |_  __  / / /_/ / /_/ /  / / /_/ /  ,<
      |/_/ /_/  \____/\____//_/  \____//_/|_|
      |
      |""".stripMargin)

  logInfo(s"Parsing Hoolok YAML file located at: '${args.head}'")
  private val config = yaml.parser.parse(new FileReader(args.head))
    .leftMap(err => err: Error)
    .flatMap(_.as[HoolokConfig])
    .valueOr(err => throw new InvalidConfigException(
      message = "Cannot parse YAML file.",
      cause = err
    ))

  logInfo("Executing Spark job with this job config: \n" + config)
  try {
    new JobRunner(config).execute()
  } catch {
    case he: HoolokException =>
      logError(he.getMessage, he)
      sys.exit(he.error.code)
    case t: Throwable =>
      logError("Unexpected error.", t)
      sys.exit(UnknownHoolokError.code)
  }

}
