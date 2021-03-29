package com.github.mmolimar

import com.amazon.deequ.analyzers._

package object hoolok {

  sealed trait Config

  case class HoolokConfig(
                           app: HoolokAppConfig,
                           schemas: Option[List[HoolokSchemaConfig]],
                           inputs: List[HoolokInputConfig],
                           steps: Option[List[HoolokStepConfig]],
                           outputs: List[HoolokOutputConfig]
                         ) extends Config

  case class HoolokAppConfig(
                              name: String,
                              enableHiveSupport: Option[Boolean],
                              sparkConf: Option[Map[String, String]],
                              sparkContext: Option[HoolokSparkContextConfig]
                            ) extends Config

  case class HoolokSparkContextConfig(
                                       archive: Option[String],
                                       description: Option[String],
                                       file: Option[String],
                                       jar: Option[String],
                                       hadoopConfiguration: Option[Map[String, String]],
                                       checkpointDir: Option[String],
                                     ) extends Config

  case class HoolokSchemaConfig(
                                 id: String,
                                 kind: String,
                                 format: String,
                                 options: Option[Map[String, String]]
                               ) extends Config

  case class HoolokInputConfig(
                                id: String,
                                format: String,
                                kind: String,
                                subtype: String,
                                schema: Option[String],
                                coalesce: Option[Int],
                                repartition: Option[HoolokRepartitionConfig],
                                watermark: Option[HoolokWatermarkConfig],
                                options: Option[Map[String, String]]
                              ) extends Config

  case class HoolokStepConfig(
                               id: String,
                               kind: String,
                               options: Option[Map[String, String]],
                               dq: Option[HoolokStepDataQualityConfig]
                             ) extends Config

  case class HoolokStepDataQualityConfig(
                                          analysis: Option[HoolokStepDataQualityAnalysisConfig],
                                          verification: Option[HoolokStepDataQualityVerificationConfig]
                                        ) extends Config

  case class HoolokStepDataQualityAnalysisConfig(
                                                  name: String,
                                                  analyzers: HoolokStepDataQualityAnalyzerConfig
                                                ) extends Config

  case class HoolokStepDataQualityAnalyzerConfig(
                                                  compliance: Option[Compliance],
                                                  completeness: Option[Completeness],
                                                  distinctness: Option[Distinctness],
                                                  uniqueness: Option[Uniqueness],
                                                  uniqueValueRatio: Option[UniqueValueRatio],
                                                  countDistinct: Option[CountDistinct],
                                                  entropy: Option[Entropy],
                                                  mutualInformation: Option[MutualInformation],
                                                  maximum: Option[Maximum],
                                                  minimum: Option[Minimum],
                                                  maxLength: Option[MaxLength],
                                                  minLength: Option[MinLength],
                                                  mean: Option[Mean],
                                                  sum: Option[Sum],
                                                  size: Option[Size],
                                                  dataType: Option[DataType]
                                                )

  case class HoolokStepDataQualityVerificationConfig(
                                                      name: String,
                                                      checks: List[HoolokStepDataQualityCheckConfig]
                                                    ) extends Config

  case class HoolokStepDataQualityCheckConfig(
                                               level: String,
                                               description: String,
                                               isComplete: Option[HoolokStepDataQualityCheckIsCompleteConfig],
                                               isUnique: Option[HoolokStepDataQualityCheckIsUniqueConfig],
                                               hasSize: Option[HoolokStepDataQualityCheckHasSizeConfig],
                                               isContainedIn: Option[HoolokStepDataQualityCheckIsContainedInConfig],
                                               isNonNegative: Option[HoolokStepDataQualityCheckIsNonNegativeConfig],
                                               isPositive: Option[HoolokStepDataQualityCheckIsPositiveConfig],
                                               isLessThan: Option[HoolokStepDataQualityCheckIsLessThanConfig],
                                               isLessThanOrEqualTo: Option[HoolokStepDataQualityCheckIsLessThanOrEqualConfig],
                                               isGreaterThan: Option[HoolokStepDataQualityCheckIsGreaterThanConfig],
                                               isGreaterThanOrEqualTo: Option[HoolokStepDataQualityCheckIsGreaterThanOrEqualConfig],
                                               hasPattern: Option[HoolokStepDataQualityCheckHasPatternConfig],
                                               containsURL: Option[HoolokStepDataQualityCheckContainsUrlConfig],
                                               containsEmail: Option[HoolokStepDataQualityCheckContainsEmailConfig],
                                             ) extends Config


  case class HoolokStepDataQualityCheckIsCompleteConfig(
                                                         column: String
                                                       ) extends Config

  case class HoolokStepDataQualityCheckIsUniqueConfig(
                                                       column: String
                                                     ) extends Config

  case class HoolokStepDataQualityCheckHasSizeConfig(
                                                      op: String,
                                                      value: Long
                                                    ) extends Config

  case class HoolokStepDataQualityCheckIsContainedInConfig(
                                                            column: String,
                                                            allowedValues: List[String]
                                                          ) extends Config

  case class HoolokStepDataQualityCheckIsNonNegativeConfig(
                                                            column: String
                                                          ) extends Config

  case class HoolokStepDataQualityCheckIsPositiveConfig(
                                                         column: String
                                                       ) extends Config

  case class HoolokStepDataQualityCheckIsLessThanConfig(
                                                         columnA: String,
                                                         columnB: String,
                                                       ) extends Config

  case class HoolokStepDataQualityCheckIsLessThanOrEqualConfig(
                                                                columnA: String,
                                                                columnB: String,
                                                              ) extends Config

  case class HoolokStepDataQualityCheckIsGreaterThanConfig(
                                                            columnA: String,
                                                            columnB: String,
                                                          ) extends Config

  case class HoolokStepDataQualityCheckIsGreaterThanOrEqualConfig(
                                                                   columnA: String,
                                                                   columnB: String,
                                                                 ) extends Config

  case class HoolokStepDataQualityCheckHasPatternConfig(
                                                         column: String,
                                                         pattern: String
                                                       ) extends Config

  case class HoolokStepDataQualityCheckContainsUrlConfig(
                                                          column: String
                                                        ) extends Config

  case class HoolokStepDataQualityCheckContainsEmailConfig(
                                                            column: String
                                                          ) extends Config

  case class HoolokOutputConfig(
                                 id: String,
                                 format: String,
                                 mode: String,
                                 kind: String,
                                 subtype: String,
                                 schema: Option[String],
                                 coalesce: Option[Int],
                                 repartition: Option[HoolokRepartitionConfig],
                                 watermark: Option[HoolokWatermarkConfig],
                                 options: Option[Map[String, String]],
                                 partitionBy: Option[List[String]],
                                 trigger: Option[String]
                               ) extends Config

  case class HoolokRepartitionConfig(
                                      numPartitions: Option[Int],
                                      partitionExprs: Option[List[String]]
                                    ) extends Config


  case class HoolokWatermarkConfig(
                                    eventTime: String,
                                    delayThreshold: String
                                  ) extends Config

}
