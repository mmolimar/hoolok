package com.github.mmolimar.hoolok.schemas

import com.github.mmolimar.hoolok.HoolokSchemaConfig
import com.github.mmolimar.hoolok.common.InvalidSchemaConfigException
import com.github.mmolimar.hoolok.common.Utils.inspectSchemas
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

trait Schema {

  val config: HoolokSchemaConfig

  def register(): Unit

}

object SchemaManager {

  private var _schemas: Map[String, StructType] = Map.empty

  private[schemas] def registerSchema(name: String, schema: StructType): Unit = {
    _schemas += (name -> schema)
  }

  def getSchema(name: String): StructType = _schemas(name)

}

object SchemaFactory {

  private val schemas = inspectSchemas

  def apply(config: HoolokSchemaConfig)(implicit spark: SparkSession): Schema = {
    schemas.get(config.kind.trim.toLowerCase)
      .map(clz => clz.getConstructor(classOf[HoolokSchemaConfig], classOf[SparkSession]))
      .map(_.newInstance(config, spark))
      .getOrElse {
        throw new InvalidSchemaConfigException(s"Schema kind '${config.kind}' is not supported.")
      }
  }

}
