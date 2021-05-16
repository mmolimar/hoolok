package com.github.mmolimar.hoolok.dq

import com.amazon.deequ.analyzers.runners.{AnalysisRunner, AnalyzerContext}
import com.amazon.deequ.analyzers.{Analyzer, State}
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.metrics.Metric
import com.amazon.deequ.{VerificationResult, VerificationSuite}
import com.github.mmolimar.hoolok._
import com.github.mmolimar.hoolok.common.{DataQualityValidationException, InvalidDataQualityConfigException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

class DeequValidation(config: HoolokDataQualityConfig)
                     (implicit spark: SparkSession) extends DataQualityValidation with Logging {

  def applyDataQuality(kind: String, id: String, dataframe: DataFrame): Unit = {
    config.analysis.foreach { a =>
      val result = analyze(a.analyzers, dataframe)
      logDebug(s"Data quality analysis report in '$kind' for " +
        s"ID '$id':\n${analysisReport(a.name, result)}")
      AnalyzerContext.successMetricsAsDataFrame(spark, result).createOrReplaceTempView(a.name)
    }

    config.verification.foreach { v =>
      val result = check(v.checks, dataframe)
      result.status match {
        case CheckStatus.Success =>
          logInfo(s"Data quality in '$kind' for ID '$id' has been successfully validated.")
        case CheckStatus.Warning =>
          logWarning(s"There are some data quality rules that have not been validated in '$kind' " +
            s"for ID '$id'. Report: \n${verificationReport(result)}")
        case CheckStatus.Error =>
          throw new DataQualityValidationException("There are some data quality rules that have not been validated " +
            s"in '$kind' for ID '$id'. Report: \n${verificationReport(result)}")
      }
      VerificationResult.successMetricsAsDataFrame(spark, result).createOrReplaceTempView(v.name)
    }
  }

  protected def analysisReport(name: String, result: AnalyzerContext): String = {
    val summary = result.allMetrics.map { m =>
      s"""
         |        - Entity: ${m.entity}
         |        - Instance: ${m.instance}
         |        - Name: ${m.name}
         |        - Value: ${m.value}
         |""".stripMargin
    }.mkString
    s"""
       |Analysis report: $name
       |Summary: $summary
       |""".stripMargin
  }

  protected def verificationReport(result: VerificationResult): String = {
    result.checkResults
      .map {
        case (check, result) =>
          val summary = result.constraintResults
            .map { c =>
              s"""
                 |        - Type: ${c.constraint}
                 |        - Status: ${c.status}
                 |        - Message: ${c.message.getOrElse("")}
                 |""".stripMargin
            }.mkString
          s"""
             |Check: ${check.description}
             |Level: ${check.level}
             |Status: ${result.status}
             |Summary: $summary
             |""".stripMargin
      }.mkString
  }

  protected def analyze[S <: State[_], M <: Metric[_]](
                                                        analyzersConfig: HoolokDataQualityAnalyzerConfig,
                                                        dataframe: DataFrame
                                                      ): AnalyzerContext = {
    val analyzers = analyzersConfig.productIterator
      .flatMap {
        case Some(a: Analyzer[_, _]) => Some(a.asInstanceOf[Analyzer[S, M]])
        case _ => None
      }
      .toSeq

    AnalysisRunner
      .onData(dataframe)
      .addAnalyzers(analyzers)
      .run()
  }

  protected def check(verification: List[HoolokDataQualityCheckConfig], dataframe: DataFrame): VerificationResult = {
    val checks = verification
      .map { cfg =>
        val level = CheckLevel.withName(cfg.level.toLowerCase.capitalize)
        val opts = cfg.productIterator
          .flatMap {
            case Some(cfg: Config) => Some(cfg)
            case _ => None
          }
        opts.foldLeft(Check(level, cfg.description))((c, o) => addConstraint(c, o))
      }
    VerificationSuite()
      .onData(dataframe)
      .addChecks(checks)
      .run()
  }

  // scalastyle:off
  private def addConstraint(check: Check, cfg: Config): Check = {
    def mapOp[A](op: String, value: A)(implicit ev: A => Ordered[A]): A => Boolean = op match {
      case "==" => _ == value
      case ">" => _ > value
      case ">=" => _ >= value
      case "<" => _ < value
      case "<=" => _ <= value
      case "!=" => _ != value
      case other: String => throw new InvalidDataQualityConfigException(s"Op '$other' is not supported.")
    }

    cfg match {
      case c: HoolokDataQualityCheckIsCompleteConfig => check.isComplete(c.column)
      case c: HoolokDataQualityCheckIsUniqueConfig => check.isUnique(c.column)
      case c: HoolokDataQualityCheckHasSizeConfig => check.hasSize(mapOp(c.op, c.value))
      case c: HoolokDataQualityCheckIsContainedInConfig => check.isContainedIn(c.column, c.allowedValues.toArray)
      case c: HoolokDataQualityCheckIsNonNegativeConfig => check.isNonNegative(c.column)
      case c: HoolokDataQualityCheckIsPositiveConfig => check.isPositive(c.column)
      case c: HoolokDataQualityCheckIsLessThanConfig => check.isLessThan(c.columnA, c.columnB)
      case c: HoolokDataQualityCheckIsLessThanOrEqualConfig => check.isLessThanOrEqualTo(c.columnA, c.columnB)
      case c: HoolokDataQualityCheckIsGreaterThanConfig => check.isGreaterThan(c.columnA, c.columnB)
      case c: HoolokDataQualityCheckIsGreaterThanOrEqualConfig => check.isGreaterThanOrEqualTo(c.columnA, c.columnB)
      case c: HoolokDataQualityCheckHasPatternConfig => check.hasPattern(c.column, c.pattern.r)
      case c: HoolokDataQualityCheckContainsUrlConfig => check.containsURL(c.column)
      case c: HoolokDataQualityCheckContainsEmailConfig => check.containsEmail(c.column)
      case c: Config => throw new InvalidDataQualityConfigException(s"Config for '${c.getClass}' is not supported.")
    }
  }

}
