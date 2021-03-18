package com.github.mmolimar.hoolok

import com.github.mmolimar.hoolok.errors.{ConfigError, ConfigStepError, Errors, UnknownHoolokError}

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

class InvalidStepConfigException(
                                  message: String,
                                  cause: Throwable = None.orNull
                                ) extends HoolokException(ConfigStepError, Some(message), cause)

class UnknownHoolokError(
                          message: String,
                          cause: Throwable
                        ) extends HoolokException(UnknownHoolokError, Some(message), cause)

// scalastyle:off
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

  case object ConfigStepError extends Errors {
    override val code: Int = -102
    override val message: String = "The configuration for the step is incorrect"
  }

  case object UnknownHoolokError extends Errors {
    override val code: Int = -900
    override val message: String = "Unexpected error when executing job"
  }

}
