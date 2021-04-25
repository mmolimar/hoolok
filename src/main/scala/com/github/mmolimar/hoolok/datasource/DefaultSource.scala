package com.github.mmolimar.hoolok.datasource

import com.github.mmolimar.hoolok.common.InvalidInputConfigException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

class DefaultSource
  extends RelationProvider
    with SchemaRelationProvider
    with DataSourceRegister
    with StreamSourceProvider
    with Logging {

  override def shortName(): String = "mock"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, None.orNull)
  }

  override def createRelation(
                               sqlContext: SQLContext,
                               parameters: Map[String, String],
                               schema: StructType): BaseRelation = {
    val mockSchema = Option(schema).getOrElse {
      throw new InvalidInputConfigException(s"Schema must be provided for datasource '${shortName()}'.")
    }
    new HoolokMockRelation(numRows(parameters), mockSchema, locale(parameters), fakers(parameters))(sqlContext)
  }

  override def sourceSchema(
                             sqlContext: SQLContext,
                             schema: Option[StructType],
                             providerName: String,
                             parameters: Map[String, String]): (String, StructType) = {
    (shortName(), schema.getOrElse {
      throw new InvalidInputConfigException(s"Schema must be provided for datasource '${shortName()}'.")
    })
  }

  override def createSource(
                             sqlContext: SQLContext,
                             metadataPath: String,
                             schema: Option[StructType],
                             providerName: String,
                             parameters: Map[String, String]): Source = {
    val sourceSchema = schema.getOrElse {
      throw new InvalidInputConfigException(s"Schema must be provided for datasource '${shortName()}'.")
    }
    new HoolokMockSource(numRows(parameters), sourceSchema, locale(parameters), fakers(parameters))(sqlContext)
  }

  private def numRows(parameters: Map[String, String]): Int = parameters.get("numRows").map(_.toInt).getOrElse(20)

  private def locale(parameters: Map[String, String]): Option[String] = parameters.get("locale")

  private val fakerRegex = "([a-zA-Z0-9_]+):(([a-zA-Z]+)\\.([a-zA-Z]+))".r

  private[datasource] def fakers(parameters: Map[String, String]): Map[String, FakerMethod] = {
    parameters.get("fakers")
      .map(_.trim.split("[,;]")
        .map(_.trim)
        .filter(_.nonEmpty)
        .map {
          case fakerRegex(fieldName, _, className, methodName) =>
            fieldName.toLowerCase.trim -> FakerMethod(className, methodName)
          case f: String =>
            throw new InvalidInputConfigException("Faker mapping config must have two values separated with a " +
              s"semicolon and the corresponding class and method name to be called. Got: '$f'. " +
              "For instance: 'field_title:Book.title'.")
        }
        .toMap
      ).getOrElse(Map.empty)
  }

}
