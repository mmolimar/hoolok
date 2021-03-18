package com.github.mmolimar

package object hoolok {

  sealed trait Config

  private[hoolok] case class HoolokConfig(
                                           app: HoolokAppConfig,
                                           inputs: List[HoolokInputConfig],
                                           steps: List[HoolokStepConfig],
                                           outputs: List[HoolokOutputConfig]
                                         ) extends Config

  private[hoolok] case class HoolokAppConfig(
                                              name: String,
                                              description: Option[String],
                                              sparkConf: Option[Map[String, String]]
                                            ) extends Config


  private[hoolok] case class HoolokInputConfig(
                                                id: String,
                                                format: String,
                                                stream: Option[Boolean],
                                                coalesce: Option[Int],
                                                repartition: Option[HoolokRepartitionConfig],
                                                options: Option[Map[String, String]]
                                              ) extends Config

  private[hoolok] case class HoolokStepConfig(
                                               id: String,
                                               `type`: String,
                                               options: Option[Map[String, String]]
                                             ) extends Config

  private[hoolok] case class HoolokOutputConfig(
                                                 id: String,
                                                 format: String,
                                                 mode: String,
                                                 coalesce: Option[Int],
                                                 repartition: Option[HoolokRepartitionConfig],
                                                 stream: Option[Boolean],
                                                 options: Option[Map[String, String]],
                                                 partitionBy: Option[List[String]],
                                                 trigger: Option[String]
                                               ) extends Config

  private[hoolok] case class HoolokRepartitionConfig(
                                                      numPartitions: Option[Int],
                                                      partitionExprs: Option[List[String]]
                                                    ) extends Config

}
