package com.github.mmolimar.hoolok

import com.github.mmolimar.hoolok.errors.{ConfigError, ConfigInputError, ConfigOutputError, ConfigStepError, Errors, UnknownHoolokError}

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

class UnknownHoolokError(
                          message: String,
                          cause: Throwable
                        ) extends HoolokException(UnknownHoolokError, Some(message), cause)

private[hoolok] object errors {

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

  case object ConfigInputError extends Errors {
    override val code: Int = -102
    override val message: String = "The configuration for the input is incorrect"
  }

  case object ConfigStepError extends Errors {
    override val code: Int = -103
    override val message: String = "The configuration for the step is incorrect"
  }

  case object ConfigOutputError extends Errors {
    override val code: Int = -104
    override val message: String = "The configuration for the output is incorrect"
  }

  case object UnknownHoolokError extends Errors {
    override val code: Int = -900
    override val message: String = "Unexpected error when executing job"
  }

}
