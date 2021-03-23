package io.circe.yaml

// scalastyle:off
object hparser {

  import cats.syntax.either._
  import io.circe._
  import org.yaml.snakeyaml.Yaml
  import org.yaml.snakeyaml.constructor.SafeConstructor
  import org.yaml.snakeyaml.env.EnvScalarConstructor
  import org.yaml.snakeyaml.nodes._

  import java.io.Reader
  import scala.collection.JavaConverters._

  def parse(yaml: Reader): Either[ParsingFailure, Json] = for {
    parsed <- parseSingle(yaml)
    json <- yamlToJson(parsed)
  } yield json

  def parseSingle(reader: Reader): Either[ParsingFailure, Node] =
    Either.catchNonFatal(new Yaml().compose(reader)).leftMap(err => ParsingFailure(err.getMessage, err))

  private[this] object CustomTag {
    def unapply(tag: Tag): Option[String] = if (!tag.startsWith(Tag.PREFIX))
      Some(tag.getValue)
    else
      None
  }

  private[this] class FlatteningConstructor extends SafeConstructor {
    def flatten(node: MappingNode): MappingNode = {
      flattenMapping(node)
      node
    }

    def construct(node: ScalarNode): Object =
      getConstructor(node).construct(node)
  }

  private val envVarPattern = "(.*)\\$\\{\\s*((\\w+)((:?([-?]))(\\S+)?)?)\\s*}(.*)".r
  private val envVarConstructor = new EnvScalarConstructor()

  private def resolveEnvVar(name: String, separator: String, value: String): String = {
    envVarConstructor.apply(name, separator, Option(value).getOrElse(""), envVarConstructor.getEnv(name));
  }

  private[this] def yamlToJson(node: Node): Either[ParsingFailure, Json] = {
    // Isn't thread-safe internally, may hence not be shared
    val flattener: FlatteningConstructor = new FlatteningConstructor

    def convertScalarNode(node: ScalarNode) = Either
      .catchNonFatal(node.getTag match {
        case Tag.INT if node.getValue.startsWith("0x") || node.getValue.contains("_") =>
          Json.fromJsonNumber(flattener.construct(node) match {
            case int: Integer => JsonLong(int.toLong)
            case long: java.lang.Long => JsonLong(long)
            case bigint: java.math.BigInteger =>
              JsonDecimal(bigint.toString)
            case other => throw new NumberFormatException(s"Unexpected number type: ${other.getClass}")
          })
        case Tag.INT | Tag.FLOAT =>
          JsonNumber.fromString(node.getValue).map(Json.fromJsonNumber).getOrElse {
            throw new NumberFormatException(s"Invalid numeric string ${node.getValue}")
          }
        case Tag.BOOL =>
          Json.fromBoolean(flattener.construct(node) match {
            case b: java.lang.Boolean => b
            case _ => throw new IllegalArgumentException(s"Invalid boolean string ${node.getValue}")
          })
        case Tag.NULL => Json.Null
        case CustomTag(other) =>
          Json.fromJsonObject(JsonObject.singleton(other.stripPrefix("!"), Json.fromString(node.getValue)))
        case _ =>
          val nodeValue = node.getValue match {
            case envVarPattern(pre, _, name, _, separator, _, value, post) =>
              s"$pre${resolveEnvVar(name, separator, value)}$post"
            case _ => node.getValue
          }
          Json.fromString(nodeValue)
      })
      .leftMap { err =>
        ParsingFailure(err.getMessage, err)
      }

    def convertKeyNode(node: Node) = node match {
      case scalar: ScalarNode => Right(scalar.getValue)
      case _ => Left(ParsingFailure("Only string keys can be represented in JSON", null))
    }

    if (node == null) {
      Right(Json.False)
    } else {
      node match {
        case mapping: MappingNode =>
          flattener
            .flatten(mapping)
            .getValue
            .asScala
            .foldLeft(
              Either.right[ParsingFailure, JsonObject](JsonObject.empty)
            ) { (objEither, tup) =>
              for {
                obj <- objEither
                key <- convertKeyNode(tup.getKeyNode)
                value <- yamlToJson(tup.getValueNode)
              } yield obj.add(key, value)
            }
            .map(Json.fromJsonObject)
        case sequence: SequenceNode =>
          sequence.getValue.asScala
            .foldLeft(Either.right[ParsingFailure, List[Json]](List.empty[Json])) { (arrEither, node) =>
              for {
                arr <- arrEither
                value <- yamlToJson(node)
              } yield value :: arr
            }
            .map(arr => Json.fromValues(arr.reverse))
        case scalar: ScalarNode => convertScalarNode(scalar)
      }
    }
  }

}
