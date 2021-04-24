package com.github.mmolimar.hoolok.common

import com.github.mmolimar.hoolok.dq.DeequValidation
import com.github.mmolimar.hoolok.schemas.SchemaManager
import com.github.mmolimar.hoolok.{HoolokDataQualityConfig, HoolokRepartitionConfig, HoolokWatermarkConfig}
import org.apache.spark.sql.SparkSession.Builder
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter, Trigger}
import org.apache.spark.sql.{DataFrame, DataFrameReader, Dataset, SparkSession}

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
      schema.map { s =>
        reader.schema(SchemaManager.getSchema(s).getOrElse(
          throw new InvalidInputConfigException(s"Schema '$s' does not exist.")
        ))
      }.getOrElse(reader)
    }

  }

  implicit class DataStreamReaderWithSchema(reader: DataStreamReader) {

    def possiblyWithSchema(schema: Option[String]): DataStreamReader = {
      schema.map { s =>
        reader.schema(SchemaManager.getSchema(s).getOrElse(
          throw new InvalidInputConfigException(s"Schema '$s' does not exist.")
        ))
      }.getOrElse(reader)
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
        case _ => dataframe
      }.getOrElse(dataframe)
    }

    def possiblyWithWatermark(watermark: Option[HoolokWatermarkConfig]): DataFrame = {
      watermark.map(w => dataframe.withWatermark(w.eventTime, w.delayThreshold)).getOrElse(dataframe)
    }

    def possiblyWithDataQuality(kind: String, id: String, dq: Option[HoolokDataQualityConfig])
                               (implicit spark: SparkSession): DataFrame = {
      dq.map(dq => {
        val validation = new DeequValidation(dq)
        validation.applyDataQuality(kind, id, dataframe)
        dataframe
      }).getOrElse(dataframe)
    }

  }

  implicit class DataStreamWriterEnricher[T](dsw: DataStreamWriter[T]) {

    private val once = "once( )*(\\( *\\))?".r
    private val continuous = "continuous( )*\\(( *[0-9]+ *)*\\)".r
    private val processing = "processingtime( )*\\(( *[0-9]+ *)*\\)".r

    private[common] def triggerType(trigger: String): Trigger = {
      trigger.trim.toLowerCase match {
        case once(_, _) | once(_) | once() => Trigger.Once()
        case continuous(_, interval) => Trigger.Continuous(interval.trim.toLong)
        case processing(_, interval) => Trigger.ProcessingTime(interval.trim.toLong)
        case _ => throw new InvalidOutputConfigException(s"Trigger value '${trigger}' cannot be parsed.")
      }
    }

    def possiblyWithTrigger(trigger: Option[String]): DataStreamWriter[T] = {
      trigger.map(t => dsw.trigger(triggerType(t))).getOrElse(dsw)
    }

    def possiblyWithDataQuality(kind: String, id: String, dq: Option[HoolokDataQualityConfig])
                               (implicit spark: SparkSession): DataStreamWriter[T] = {
      dq.map(dq => {
        val validation = new DeequValidation(dq)
        dsw.foreachBatch { (batchDs: Dataset[T], batchId: Long) =>
          validation.applyDataQuality(kind, s"$id-$batchId", batchDs.toDF())
        }
      }).getOrElse(dsw)
    }

    def possiblyWithCustomForeachBatch(function: Option[(Dataset[T], Long) => Unit]): DataStreamWriter[T] = {
      function.map(fn => dsw.foreachBatch(fn)).getOrElse(dsw)
    }

  }

}
