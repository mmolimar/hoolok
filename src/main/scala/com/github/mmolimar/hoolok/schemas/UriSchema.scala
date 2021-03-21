package com.github.mmolimar.hoolok.schemas

import com.github.mmolimar.hoolok.HoolokSchemaConfig
import com.github.mmolimar.hoolok.annotations.SchemaKind
import com.github.mmolimar.hoolok.common.{InvalidConfigException, SchemaReadException}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.io.InputStream
import java.net.URL
import scala.io.Source

@SchemaKind(kind = "uri")
class UriSchema(config: HoolokSchemaConfig)
               (implicit spark: SparkSession) extends BaseSchema(config)(spark) {

  val schemaPath: Path = config.options.flatMap(_.get("path").map(new Path(_))).getOrElse {
    throw new InvalidConfigException("URI schema is not configured properly. The option 'path' is expected.")
  }

  override def schemaContent: String = {
    val is: InputStream = Option(schemaPath.toUri.getScheme).map(_.toLowerCase) match {
      case Some("http") | Some("https") =>
        new URL(schemaPath.toString).openConnection().getInputStream
      case Some(_) =>
        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        if (!fs.exists(schemaPath)) {
          throw new SchemaReadException(s"Schema in path '$schemaPath' for schema ID '${config.id}' cannot " +
            "be read or does not exist.")
        }
        fs.open(schemaPath)
      case None =>
        this.getClass.getResourceAsStream(schemaPath.toString)
    }
    val content = Source.fromInputStream(is).mkString
    is.close()

    content
  }

}
