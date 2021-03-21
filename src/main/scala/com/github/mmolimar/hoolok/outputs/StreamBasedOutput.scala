package com.github.mmolimar.hoolok.outputs

import com.github.mmolimar.hoolok.HoolokOutputConfig
import com.github.mmolimar.hoolok.annotations.OutputKind
import com.github.mmolimar.hoolok.common.{StreamGracefulShutdownConfigException, StreamGracefulShutdownStopException}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.format.DateTimeFormatter
import java.util.UUID
import java.util.concurrent.{Executors, TimeUnit}

@OutputKind(kind = "stream")
class StreamBasedOutput(config: HoolokOutputConfig)
                       (implicit spark: SparkSession) extends BaseOutput(config)(spark) {

  val gracefulShutdownPath: Option[String] = config.options.flatMap(_.get("gracefulShutdownPath"))
  val gracefulShutdownFileNotCreated: Boolean = config.options
    .flatMap(_.get("gracefulShutdownFileNotCreated").map(_.toBoolean))
    .getOrElse(true)
  val defaultGracefulShutdownDelay: Int = 10
  val gracefulShutdownDelay: Int = config.options
    .flatMap(_.get("gracefulShutdownDelay").map(_.toInt))
    .getOrElse(defaultGracefulShutdownDelay)

  def writeInternal(dataframe: DataFrame): Unit = {
    val query = dataframe
      .writeStream
      .outputMode(config.mode)
      .format(config.format)
      .options(config.options.getOrElse(Map.empty))
      .partitionBy(config.partitionBy.orNull: _*)
      .start()

    gracefulShutdownPath.foreach(path => enableGracefulShutdown(query, path))
  }

  private def enableGracefulShutdown(query: StreamingQuery, path: String): Unit = {
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

  private def gracefulShutdown(query: StreamingQuery, fs: FileSystem, marker: Path): Runnable = new Runnable {

    def markerExists: Boolean = fs.exists(marker)

    override def run(): Unit = if (!markerExists && query.isActive) {
      logWarning(s"Stopping query '${query.id}' for stream with ID '${config.id}' " +
        s"due to file '$marker' does not exist.")
      query.stop()
      throw new StreamGracefulShutdownStopException(s"Stopping query '${query.id}' for stream with ID '${config.id}' " +
        s"due to file '$marker' does not exist.")
    }

  }

}
