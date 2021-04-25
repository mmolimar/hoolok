package com.github.mmolimar.hoolok

import org.apache.spark.sql.types._

package object datasource {

  val parameters: Map[String, String] = Map(
    "numRows" -> "10",
    "fakers" ->
      """
        |long:Address.streetAddressNumber;
        |string:Address.streetAddress;
        |byte:Country.name;
        |short:Number.digit;
        |integer:Address.buildingNumber;
        |float:Address.longitude;
        |double:Address.latitude;
        |nested_long:Address.streetAddressNumber;
        |nested_string:Address.streetAddress;
        |nested_byte:Country.name;
        |nested_short:Number.digit;
        |nested_integer:Address.buildingNumber;
        |nested_float:Address.longitude;
        |nested_double:Address.latitude;
        |;
        |  ;
        |
        |""".stripMargin
  )

  val schema: StructType = new StructType()
    .add("long", LongType)
    .add("string", StringType)
    .add("byte", ByteType)
    .add("boolean", BooleanType)
    .add("short", ShortType)
    .add("integer", IntegerType)
    .add("float", FloatType)
    .add("double", DoubleType)
    .add("decimal", DecimalType.apply(10, 10))
    .add("nulltype", NullType)
    .add("ts", TimestampType)
    .add("date", DateType)
    .add("calendar", CalendarIntervalType)
    .add("array", ArrayType(CalendarIntervalType))
    .add("map", MapType(StringType, DateType))
    .add("struct", new StructType()
      .add("nested_string", StringType)
      .add("nested_long", LongType)
      .add("nested_string", StringType)
      .add("nested_byte", ByteType)
      .add("nested_boolean", BooleanType)
      .add("nested_short", ShortType)
      .add("nested_integer", IntegerType)
      .add("nested_float", FloatType)
      .add("nested_double", DoubleType)
      .add("nested_decimal", DecimalType.apply(10, 10))
      .add("nested_nulltype", NullType)
      .add("nested_ts", TimestampType)
      .add("nested_date", DateType)
      .add("nested_calendar", CalendarIntervalType)
      .add("nested_array", ArrayType.apply(CalendarIntervalType))
      .add("nested_map", MapType(StringType, DateType))
    )

}
