package com.github.mmolimar.hoolok.schemas

import com.github.mmolimar.hoolok.HoolokSchemaConfig
import com.github.mmolimar.hoolok.annotations.SchemaKind
import com.github.mmolimar.hoolok.common.{InvalidSchemaConfigException, SchemaReadException}
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters.seqAsJavaList
import scala.util.{Failure, Success, Try}

@SchemaKind(kind = "schema-registry")
class SchemaRegistrySchema(config: HoolokSchemaConfig)
                          (implicit spark: SparkSession) extends BaseSchema(config)(spark) {

  private val mapCapacity = 10

  val baseUrls: Seq[String] = config.options.flatMap(_.get("schemaRegistryUrls")).getOrElse {
    throw new InvalidSchemaConfigException(s"Schema '${config.kind}' is not configured properly " +
      "for retrieving schemas from Schema Registry. The option 'schemaRegistryUrls' is expected.")
  }.split(",")
  val schemaRegistry = new CachedSchemaRegistryClient(seqAsJavaList(baseUrls), mapCapacity)

  val schemaSubject: Option[String] = config.options.flatMap(_.get("subject"))
  val schemaId: Option[Int] = config.options.flatMap(_.get("id").map(_.toInt))
  if (schemaSubject.isEmpty && schemaId.isEmpty) {
    throw new InvalidSchemaConfigException(s"Schema '${config.kind}' is not configured " +
      "properly for retrieving schemas. The options 'id' and/or 'subject' are expected.")
  }

  override def schemaContent: String = Try {
    (schemaSubject, schemaId) match {
      case (Some(subject), Some(id)) =>
        logInfo(s"Retrieving schema '${config.kind}' with id '$id' and subject '$subject'.")
        Option(schemaRegistry.getBySubjectAndId(subject, id))
      case (None, Some(id)) =>
        logInfo(s"Retrieving schema '${config.kind}' with id '$id'.")
        Option(schemaRegistry.getById(id))
      case (Some(subject), None) =>
        logInfo(s"Retrieving latest schema '${config.kind}' with subject '$subject'.")
        val id = schemaRegistry.getLatestSchemaMetadata(subject).getId
        Option(schemaRegistry.getBySubjectAndId(subject, id))
      case _ => throw new InvalidSchemaConfigException(s"Schema '${config.kind}' is not configured " +
        "properly for retrieving schemas. The options 'id' and/or 'subject' are expected.")
    }
  } match {
    case Success(Some(schema)) => schema.toString
    case Success(None) =>
      throw new SchemaReadException(s"Schema '${config.kind}' for ID '${config.id}' cannot " +
        "be read or does not exist.")
    case Failure(e) =>
      throw new SchemaReadException(s"Schema '${config.kind}' for ID '${config.id}' cannot " +
        "be read or does not exist.", e)
  }

}
