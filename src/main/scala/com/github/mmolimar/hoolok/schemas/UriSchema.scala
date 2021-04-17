package com.github.mmolimar.hoolok.schemas

import com.github.mmolimar.hoolok.HoolokSchemaConfig
import com.github.mmolimar.hoolok.annotations.SchemaKind
import com.github.mmolimar.hoolok.common.Utils.closer
import com.github.mmolimar.hoolok.common.{InvalidSchemaConfigException, SchemaReadException}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.net.URL
import scala.io.Source
import scala.util.{Failure, Success, Try}

@SchemaKind(kind = "uri")
class UriSchema(config: HoolokSchemaConfig)
               (implicit spark: SparkSession) extends BaseSchema(config)(spark) {

  val schemaPath: Path = config.options.flatMap(_.get("path").map(new Path(_))).getOrElse {
    throw new InvalidSchemaConfigException(s"Schema '${config.kind}' is not configured properly. " +
      "The option 'path' is expected.")
  }

  override def schemaContent: String = {
    Try {
      Option(schemaPath.toUri.getScheme).map(_.toLowerCase) match {
        case Some("http") | Some("https") =>
          Option(new URL(schemaPath.toString).openConnection().getInputStream)
        case Some(_) =>
          val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
          Option(fs.open(schemaPath))
        case None =>
          Option(getClass.getResourceAsStream(schemaPath.toString))
      }
    } match {
      case Success(Some(is)) =>
        closer(is)(is => Source.fromInputStream(is).mkString)
      case Success(None) =>
        throw new SchemaReadException(s"Schema '${config.kind}' in path '$schemaPath' for schema ID '${config.id}' " +
          "cannot be read or does not exist.")
      case Failure(e) =>
        throw new SchemaReadException(s"Schema '${config.kind}' in path '$schemaPath' for schema ID '${config.id}' " +
          "cannot be read or does not exist.", e)
    }
  }

}
