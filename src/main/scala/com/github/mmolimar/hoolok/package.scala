package com.github.mmolimar

package object hoolok {

  sealed trait Config

  private[hoolok] case class HoolokConfig(
                                           app: HoolokAppConfig,
                                           schemas: Option[List[HoolokSchemaConfig]],
                                           inputs: List[HoolokInputConfig],
                                           steps: List[HoolokStepConfig],
                                           outputs: List[HoolokOutputConfig]
                                         ) extends Config

  private[hoolok] case class HoolokAppConfig(
                                              name: String,
                                              enableHiveSupport: Option[Boolean],
                                              sparkConf: Option[Map[String, String]],
                                              sparkContext: Option[HoolokSparkContextConfig]
                                            ) extends Config

  private[hoolok] case class HoolokSparkContextConfig(
                                                       archive: Option[String],
                                                       description: Option[String],
                                                       file: Option[String],
                                                       jar: Option[String],
                                                       hadoopConfiguration: Option[Map[String, String]]
                                                     ) extends Config

  private[hoolok] case class HoolokSchemaConfig(
                                                 id: String,
                                                 kind: String,
                                                 format: String,
                                                 options: Option[Map[String, String]]
                                               ) extends Config

  private[hoolok] case class HoolokInputConfig(
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

  private[hoolok] case class HoolokStepConfig(
                                               id: String,
                                               kind: String,
                                               options: Option[Map[String, String]]
                                             ) extends Config

  private[hoolok] case class HoolokOutputConfig(
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

  private[hoolok] case class HoolokRepartitionConfig(
                                                      numPartitions: Option[Int],
                                                      partitionExprs: Option[List[String]]
                                                    ) extends Config


  private[hoolok] case class HoolokWatermarkConfig(
                                                    eventTime: String,
                                                    delayThreshold: String
                                                  ) extends Config

}
