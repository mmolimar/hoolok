package com.github.mmolimar.hoolok.outputs

import com.github.mmolimar.hoolok.HoolokOutputConfig
import com.github.mmolimar.hoolok.annotations.OutputStreamKind
import com.github.mmolimar.hoolok.common.Implicits.DataStreamWriterEnricher
import com.github.mmolimar.hoolok.common.{InvalidOutputConfigException, StreamGracefulShutdownConfigException, StreamGracefulShutdownStopException}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.time.format.DateTimeFormatter
import java.util.UUID
import java.util.concurrent.{Executors, TimeUnit}

abstract class StreamBasedOutput(config: HoolokOutputConfig)
                                (implicit spark: SparkSession) extends BaseOutput(config)(spark) {

  val gracefulShutdownPath: Option[String] = config.options.flatMap(_.get("gracefulShutdownPath"))
  val gracefulShutdownFileNotCreated: Boolean = config.options
    .flatMap(_.get("gracefulShutdownFileNotCreated").map(_.toBoolean))
    .getOrElse(true)
  val defaultGracefulShutdownDelay: Int = 10
  val gracefulShutdownDelay: Int = config.options
    .flatMap(_.get("gracefulShutdownDelay").map(_.toInt))
    .getOrElse(defaultGracefulShutdownDelay)

  override final def writeInternal(dataframe: DataFrame): Unit = {
    val query = save(
      dataframe
        .writeStream
        .outputMode(config.mode)
        .format(config.format)
        .options(config.options.getOrElse(Map.empty))
        .partitionBy(config.partitionBy.orNull: _*)
        .possiblyWithTrigger(config.trigger)
        .possiblyWithDataQuality(config.kind, config.id, config.dq)
        .possiblyWithCustomForeachBatch(customForeachBatch)
    )

    gracefulShutdownPath.foreach(path => enableGracefulShutdown(query, path))
  }

  protected def customForeachBatch[T]: Option[(Dataset[T], Long) => Unit] = None

  def save[T](dfw: DataStreamWriter[T]): StreamingQuery

  protected def enableGracefulShutdown(query: StreamingQuery, path: String): Unit = {
    val fileName: String = {
      val dateFormat = DateTimeFormatter.ofPattern("YYYYMMdd_HHmmss").format(java.time.LocalDateTime.now)
      s"hoolok_${spark.sparkContext.appName}_${config.id}_${dateFormat}_${UUID.randomUUID()}.stream"
    }
    val initialDelay: Int = 10
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val dir = new Path(path)
    val markerPath = new Path(dir, fileName)
    fs.mkdirs(dir)

    if (!fs.createNewFile(markerPath)) {
      logWarning(s"Graceful shutdown file could not be created in '$markerPath' " +
        s"for stream with ID '${config.id}'.")
      if (gracefulShutdownFileNotCreated) {
        throw new StreamGracefulShutdownConfigException(s"Graceful shutdown file '$markerPath' could not be created " +
          s"for stream with ID '${config.id}'")
      }
    } else {
      logInfo(s"Created file in '$markerPath' as graceful shutdown marker for stream with ID '${config.id}'.")
      Executors.newSingleThreadScheduledExecutor
        .scheduleWithFixedDelay(gracefulShutdown(query, fs, markerPath), initialDelay,
          gracefulShutdownDelay, TimeUnit.SECONDS)
    }

  }

  protected def gracefulShutdown(query: StreamingQuery, fs: FileSystem, marker: Path): Runnable = new Runnable {

    def markerExists: Boolean = fs.exists(marker)

    override def run(): Unit = if (query.isActive && !markerExists) {
      logWarning(s"Stopping query '${query.id}' for stream with ID '${config.id}' " +
        s"due to file '$marker' does not exist.")
      query.stop()
      throw new StreamGracefulShutdownStopException(s"Stopping query '${query.id}' for stream with ID '${config.id}' " +
        s"due to file '$marker' does not exist.")
    }

  }

}

@OutputStreamKind(subtype = "table")
class TableStreamOutput(config: HoolokOutputConfig)
                       (implicit spark: SparkSession) extends StreamBasedOutput(config)(spark) {

  val tableName: String = config.options.flatMap(_.get("tableName")).getOrElse {
    throw new InvalidOutputConfigException(s"Table stream output for ID '${config.id}' is not configured properly. " +
      "The option 'tableName' is expected.")
  }

  override def save[T](dfw: DataStreamWriter[T]): StreamingQuery = dfw.toTable(tableName)

}

@OutputStreamKind(subtype = "data-source")
class DataSourceStreamOutput(config: HoolokOutputConfig)
                            (implicit spark: SparkSession) extends StreamBasedOutput(config)(spark) {

  override def save[T](dfw: DataStreamWriter[T]): StreamingQuery = dfw.start()

}
