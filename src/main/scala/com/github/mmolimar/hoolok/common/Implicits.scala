package com.github.mmolimar.hoolok.common

import com.github.mmolimar.hoolok.schemas.SchemaManager
import com.github.mmolimar.hoolok.{HoolokRepartitionConfig, HoolokWatermarkConfig}
import org.apache.spark.sql.SparkSession.Builder
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.{DataFrame, DataFrameReader}

object Implicits {

  implicit class SparkSessionBuilderOptions(builder: Builder) {

    def withHiveSupport(enable: Option[Boolean]): Builder = {
      if (!enable.getOrElse(false)) {
        builder.enableHiveSupport()
      } else {
        builder
      }
    }

    def withSparkConf(sparkConf: Map[String, String]): Builder = {
      sparkConf.foldLeft(builder)((builder, conf) => builder.config(conf._1, conf._2))
    }

  }

  implicit class DataFrameReaderWithSchema(reader: DataFrameReader) {

    def possiblyWithSchema(schema: Option[String]): DataFrameReader = {
      schema.map(s => reader.schema(SchemaManager.getSchema(s))).getOrElse(reader)
    }

  }

  implicit class DataStreamReaderWithSchema(reader: DataStreamReader) {

    def possiblyWithSchema(schema: Option[String]): DataStreamReader = {
      schema.map(s => reader.schema(SchemaManager.getSchema(s))).getOrElse(reader)
    }

  }


  implicit class DataframeEnricher(dataframe: DataFrame) {

    def possiblyWithCoalesce(coalesce: Option[Int]): DataFrame = {
      coalesce.map(dataframe.coalesce).getOrElse(dataframe)
    }

    def possiblyWithRepartition(repartition: Option[HoolokRepartitionConfig]): DataFrame = {
      repartition.map {
        case HoolokRepartitionConfig(Some(numPartitions), None) =>
          dataframe.repartition(numPartitions)
        case HoolokRepartitionConfig(None, Some(partitionExprs)) =>
          dataframe.repartition(partitionExprs.map(col): _*)
        case HoolokRepartitionConfig(Some(numPartitions), Some(partitionExprs)) =>
          dataframe.repartition(numPartitions, partitionExprs.map(col): _*)
        case _ =>
          throw new InvalidConfigException("Repartition is not configured properly. You must configure " +
            "'numPartitions' and/or 'partitionExprs'.")
      }.getOrElse(dataframe)
    }

    def possiblyWithWatermark(watermark: Option[HoolokWatermarkConfig]): DataFrame = {
      watermark.map(w => dataframe.withWatermark(w.eventTime, w.delayThreshold)).getOrElse(dataframe)
    }

  }

}