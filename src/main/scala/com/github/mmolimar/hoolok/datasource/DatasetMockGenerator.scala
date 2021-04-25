package com.github.mmolimar.hoolok.datasource

import com.github.javafaker.Faker
import com.github.mmolimar.hoolok.common.InvalidInputConfigException
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.unsafe.types.{CalendarInterval => TypeCalendarInterval}

import java.lang.reflect.{Method, Modifier}
import java.math.MathContext
import java.sql.{Date, Timestamp}
import java.util.Locale
import scala.collection.JavaConverters._
import scala.util.Random

case class FakerMethod(className: String, methodName: String)

class DatasetMockGenerator(locale: Option[String]) {

  private val faker: Faker = locale.map(l => new Faker(Locale.forLanguageTag(l))).getOrElse(new Faker())

  lazy val fakerMethods: Map[String, Method] = faker.getClass.getMethods
    .filter(m => m.getParameterCount == 0 && Modifier.isPublic(m.getModifiers))
    .map(m => m.getName.toLowerCase -> m)
    .toMap

  def createFakeDataframe(
                           sqlContext: SQLContext,
                           numRows: Int, schema: StructType,
                           fakers: Map[String, FakerMethod]
                         ): DataFrame = {
    val generator = () => Row.fromSeq(schema.fields.map(createField(_, fakers)))
    val rows = List.fill(numRows)(generator()).asJava
    sqlContext.sparkSession.createDataFrame(rows, schema)
  }

  // scalastyle:off
  def createField(field: StructField, fakers: Map[String, FakerMethod]): Any = {
    val fakerMethod = fakers.get(field.name.toLowerCase())
    lazy val faked = fakerMethod.map(fakeValue)
    field.dataType match {
      case StringType => faked.map(_.toString).getOrElse(Random.alphanumeric.take(Random.nextInt(50)).mkString)
      case ByteType => faked.map(_.toString.head.toByte).getOrElse(Random.alphanumeric.take(1).head.toByte)
      case BooleanType => faked.map(_.toString.toBoolean).getOrElse(Random.nextBoolean())
      case ShortType => faked.map(_.toString.toShort).getOrElse(Random.nextInt().toShort)
      case IntegerType => faked.map(_.toString.toInt).getOrElse(Random.nextInt())
      case LongType => faked.map(_.toString.toLong).getOrElse(Random.nextLong())
      case FloatType => faked.map(_.toString.toFloat).getOrElse(Random.nextFloat())
      case DoubleType => faked.map(_.toString.toDouble).getOrElse(Random.nextDouble())
      case dt: DecimalType => BigDecimal.apply(
        Random.nextLong() % math.pow(10, dt.precision).toLong, dt.scale, new MathContext(dt.precision)
      ).bigDecimal
      case NullType => None.orNull
      case TimestampType =>
        val start = 21600000
        val end = 1893477600000L
        new Timestamp(start + (Random.nextDouble() * (end - start)).toLong)
      case DateType =>
        val start = 21600000
        val end = 1893477600000L
        new Date(start + (Random.nextDouble() * (end - start)).toLong)
      case CalendarIntervalType =>
        new TypeCalendarInterval(Random.nextInt(1000), Random.nextInt(10000), Random.nextLong())
      case array: ArrayType =>
        val item = StructField(field.name, array.elementType, array.containsNull)
        Seq.fill(Random.nextInt(50))(createField(item, fakers))
      case map: MapType =>
        val keyValue = createField(StructField(field.name, map.keyType, nullable = false), fakers)
        val mapValue = createField(StructField(field.name, map.valueType, map.valueContainsNull), fakers)
        Map(keyValue -> mapValue)
      case struct: StructType => Row.fromSeq(struct.fields.map(createField(_, fakers)))
      case v: VarcharType => faked.map(_.toString.sliding(v.length))
        .getOrElse(Random.alphanumeric.take(Random.nextInt(v.length)).mkString)
    }
  }

  private[datasource] def fakeValue(fakerMethod: FakerMethod): AnyRef = {
    fakerMethods.get(fakerMethod.className.toLowerCase)
      .flatMap { method =>
        val obj = method.invoke(faker)
        obj.getClass.getMethods
          .filter(m => m.getParameterCount == 0 && Modifier.isPublic(m.getModifiers))
          .find(_.getName.equalsIgnoreCase(fakerMethod.methodName))
          .map(_.invoke(obj))
      }.getOrElse {
      throw new InvalidInputConfigException(s"Class '${fakerMethod.className}' and method '${fakerMethod.methodName}' " +
        "cannot be found in the faker. See https://github.com/DiUS/java-faker")
    }
  }

}

object DatasetMockGenerator {

  def apply(locale: Option[String] = None): DatasetMockGenerator = new DatasetMockGenerator(locale)

}
