package com.github.mmolimar.hoolok.common

import com.github.mmolimar.hoolok.common.Errors._

private[hoolok] sealed class HoolokException(
                                              val error: Errors,
                                              message: Option[String],
                                              cause: Throwable = None.orNull
                                            ) extends RuntimeException(error.toString + message.getOrElse(""), cause)

class MissingConfigFileException(
                                  message: String,
                                  cause: Throwable = None.orNull
                                ) extends HoolokException(ConfigError, Some(message), cause)

class InvalidConfigException(
                              message: String,
                              cause: Throwable = None.orNull
                            ) extends HoolokException(ConfigError, Some(message), cause)

class InvalidSchemaConfigException(
                                    message: String,
                                    cause: Throwable = None.orNull
                                  ) extends HoolokException(ConfigSchemaError, Some(message), cause)

class InvalidInputConfigException(
                                   message: String,
                                   cause: Throwable = None.orNull
                                 ) extends HoolokException(ConfigInputError, Some(message), cause)

class InvalidStepConfigException(
                                  message: String,
                                  cause: Throwable = None.orNull
                                ) extends HoolokException(ConfigStepError, Some(message), cause)

class InvalidOutputConfigException(
                                    message: String,
                                    cause: Throwable = None.orNull
                                  ) extends HoolokException(ConfigOutputError, Some(message), cause)

class SchemaValidationException(
                                 message: String,
                                 cause: Throwable = None.orNull
                               ) extends HoolokException(SchemaValidationError, Some(message), cause)

class SchemaReadException(
                                 message: String,
                                 cause: Throwable = None.orNull
                               ) extends HoolokException(SchemaReadError, Some(message), cause)

class StreamGracefulShutdownConfigException(
                                             message: String,
                                             cause: Throwable = None.orNull
                                           ) extends HoolokException(StreamGracefulShutdownConfigError, Some(message), cause)

class StreamGracefulShutdownStopException(
                                           message: String,
                                           cause: Throwable = None.orNull
                                         ) extends HoolokException(StreamGracefulShutdownStopError, Some(message), cause)

class UnknownHoolokError(
                          message: String,
                          cause: Throwable
                        ) extends HoolokException(UnknownHoolokError, Some(message), cause)

private[hoolok] object Errors {

  sealed trait Errors {
    val code: Int
    val message: String

    override final def toString: String = s"$code - $message: "
  }

  case object MissingConfigError extends Errors {
    override val code: Int = -100
    override val message: String = "Config file was not provided"
  }

  case object ConfigError extends Errors {
    override val code: Int = -101
    override val message: String = "Hoolok YAML file is not valid"
  }

  case object ConfigSchemaError extends Errors {
    override val code: Int = -102
    override val message: String = "The configuration for the schema is incorrect"
  }

  case object ConfigInputError extends Errors {
    override val code: Int = -103
    override val message: String = "The configuration for the input is incorrect"
  }

  case object ConfigStepError extends Errors {
    override val code: Int = -104
    override val message: String = "The configuration for the step is incorrect"
  }

  case object ConfigOutputError extends Errors {
    override val code: Int = -105
    override val message: String = "The configuration for the output is incorrect"
  }

  case object SchemaValidationError extends Errors {
    override val code: Int = -200
    override val message: String = "The schema in the dataframe does not match with the one provided"
  }

  case object SchemaReadError extends Errors {
    override val code: Int = -201
    override val message: String = "The schema specified cannot be read"
  }

  case object StreamGracefulShutdownConfigError extends Errors {
    override val code: Int = -300
    override val message: String = "An error has occurred when configuring the graceful shutdown for a stream"
  }

  case object StreamGracefulShutdownStopError extends Errors {
    override val code: Int = -301
    override val message: String = "The query stream has been stopped"
  }

  case object UnknownHoolokError extends Errors {
    override val code: Int = -900
    override val message: String = "Unexpected error when executing job"
  }

}
