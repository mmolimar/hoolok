package com.github.mmolimar

import com.amazon.deequ.analyzers._

// scalastyle:off
package object hoolok {

  sealed trait Config

  case class HoolokConfig(
                           app: HoolokAppConfig,
                           schemas: Option[List[HoolokSchemaConfig]] = None,
                           inputs: List[HoolokInputConfig],
                           steps: Option[List[HoolokStepConfig]] = None,
                           outputs: List[HoolokOutputConfig]
                         ) extends Config

  case class HoolokAppConfig(
                              name: String,
                              enableHiveSupport: Option[Boolean] = None,
                              streamingPolicy: Option[String] = Some("default"),
                              sparkConf: Option[Map[String, String]] = None,
                              sparkContext: Option[HoolokSparkContextConfig] = None
                            ) extends Config

  case class HoolokSparkContextConfig(
                                       archive: Option[String] = None,
                                       description: Option[String] = None,
                                       file: Option[String] = None,
                                       jar: Option[String] = None,
                                       hadoopConfiguration: Option[Map[String, String]] = None,
                                       checkpointDir: Option[String] = None
                                     ) extends Config

  case class HoolokSchemaConfig(
                                 id: String,
                                 kind: String,
                                 format: String,
                                 options: Option[Map[String, String]] = None
                               ) extends Config

  case class HoolokInputConfig(
                                id: String,
                                format: String,
                                kind: String,
                                subtype: String,
                                schema: Option[String] = None,
                                coalesce: Option[Int] = None,
                                repartition: Option[HoolokRepartitionConfig] = None,
                                watermark: Option[HoolokWatermarkConfig] = None,
                                options: Option[Map[String, String]] = None
                              ) extends Config

  case class HoolokStepConfig(
                               id: String,
                               kind: String,
                               options: Option[Map[String, String]] = None,
                               dq: Option[HoolokDataQualityConfig] = None
                             ) extends Config

  case class HoolokDataQualityConfig(
                                      analysis: Option[HoolokDataQualityAnalysisConfig] = None,
                                      verification: Option[HoolokDataQualityVerificationConfig] = None
                                    ) extends Config

  case class HoolokDataQualityAnalysisConfig(
                                              name: String,
                                              analyzers: HoolokDataQualityAnalyzerConfig
                                            ) extends Config

  case class HoolokDataQualityAnalyzerConfig(
                                              compliance: Option[Compliance] = None,
                                              completeness: Option[Completeness] = None,
                                              distinctness: Option[Distinctness] = None,
                                              uniqueness: Option[Uniqueness] = None,
                                              uniqueValueRatio: Option[UniqueValueRatio] = None,
                                              countDistinct: Option[CountDistinct] = None,
                                              entropy: Option[Entropy] = None,
                                              mutualInformation: Option[MutualInformation] = None,
                                              maximum: Option[Maximum] = None,
                                              minimum: Option[Minimum] = None,
                                              maxLength: Option[MaxLength] = None,
                                              minLength: Option[MinLength] = None,
                                              mean: Option[Mean] = None,
                                              sum: Option[Sum] = None,
                                              size: Option[Size] = None,
                                              dataType: Option[DataType] = None
                                            )

  case class HoolokDataQualityVerificationConfig(
                                                  name: String,
                                                  checks: List[HoolokDataQualityCheckConfig]
                                                ) extends Config

  case class HoolokDataQualityCheckConfig(
                                           level: String,
                                           description: String,
                                           isComplete: Option[HoolokDataQualityCheckIsCompleteConfig] = None,
                                           isUnique: Option[HoolokDataQualityCheckIsUniqueConfig] = None,
                                           hasSize: Option[HoolokDataQualityCheckHasSizeConfig] = None,
                                           isContainedIn: Option[HoolokDataQualityCheckIsContainedInConfig] = None,
                                           isNonNegative: Option[HoolokDataQualityCheckIsNonNegativeConfig] = None,
                                           isPositive: Option[HoolokDataQualityCheckIsPositiveConfig] = None,
                                           isLessThan: Option[HoolokDataQualityCheckIsLessThanConfig] = None,
                                           isLessThanOrEqualTo: Option[HoolokDataQualityCheckIsLessThanOrEqualConfig] = None,
                                           isGreaterThan: Option[HoolokDataQualityCheckIsGreaterThanConfig] = None,
                                           isGreaterThanOrEqualTo: Option[HoolokDataQualityCheckIsGreaterThanOrEqualConfig] = None,
                                           hasPattern: Option[HoolokDataQualityCheckHasPatternConfig] = None,
                                           containsURL: Option[HoolokDataQualityCheckContainsUrlConfig] = None,
                                           containsEmail: Option[HoolokDataQualityCheckContainsEmailConfig] = None
                                         ) extends Config


  case class HoolokDataQualityCheckIsCompleteConfig(
                                                     column: String
                                                   ) extends Config

  case class HoolokDataQualityCheckIsUniqueConfig(
                                                   column: String
                                                 ) extends Config

  case class HoolokDataQualityCheckHasSizeConfig(
                                                  op: String,
                                                  value: Long
                                                ) extends Config

  case class HoolokDataQualityCheckIsContainedInConfig(
                                                        column: String,
                                                        allowedValues: List[String]
                                                      ) extends Config

  case class HoolokDataQualityCheckIsNonNegativeConfig(
                                                        column: String
                                                      ) extends Config

  case class HoolokDataQualityCheckIsPositiveConfig(
                                                     column: String
                                                   ) extends Config

  case class HoolokDataQualityCheckIsLessThanConfig(
                                                     columnA: String,
                                                     columnB: String
                                                   ) extends Config

  case class HoolokDataQualityCheckIsLessThanOrEqualConfig(
                                                            columnA: String,
                                                            columnB: String
                                                          ) extends Config

  case class HoolokDataQualityCheckIsGreaterThanConfig(
                                                        columnA: String,
                                                        columnB: String
                                                      ) extends Config

  case class HoolokDataQualityCheckIsGreaterThanOrEqualConfig(
                                                               columnA: String,
                                                               columnB: String
                                                             ) extends Config

  case class HoolokDataQualityCheckHasPatternConfig(
                                                     column: String,
                                                     pattern: String
                                                   ) extends Config

  case class HoolokDataQualityCheckContainsUrlConfig(
                                                      column: String
                                                    ) extends Config

  case class HoolokDataQualityCheckContainsEmailConfig(
                                                        column: String
                                                      ) extends Config

  case class HoolokOutputConfig(
                                 id: String,
                                 format: String,
                                 mode: String,
                                 kind: String,
                                 subtype: String,
                                 schema: Option[String] = None,
                                 coalesce: Option[Int] = None,
                                 repartition: Option[HoolokRepartitionConfig] = None,
                                 watermark: Option[HoolokWatermarkConfig] = None,
                                 options: Option[Map[String, String]] = None,
                                 partitionBy: Option[List[String]] = None,
                                 trigger: Option[String] = None,
                                 dq: Option[HoolokDataQualityConfig] = None
                               ) extends Config

  case class HoolokRepartitionConfig(
                                      numPartitions: Option[Int] = None,
                                      partitionExprs: Option[List[String]] = None
                                    ) extends Config


  case class HoolokWatermarkConfig(
                                    eventTime: String,
                                    delayThreshold: String
                                  ) extends Config

}
