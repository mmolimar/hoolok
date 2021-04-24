package com.github.mmolimar.hoolok

import java.nio.file.Files

package object outputs {

  val tableBatchOutputConfig: HoolokOutputConfig =
    HoolokOutputConfig(id = "id_table_batch", kind = "batch", format = "csv", subtype = "table", mode = "overwrite",
      options = Some(Map(
        "tableName" -> "batch_table"
      ))
    )
  val dataSourceBatchOutputConfig: HoolokOutputConfig =
    HoolokOutputConfig(id = "id_data_source_batch", kind = "batch", format = "csv", subtype = "data-source",
      mode = "overwrite", schema = Some("schema-test"), options = Some(Map(
        "inferSchema" -> "false"
      ))
    )
  val tableStreamOutputConfig: HoolokOutputConfig =
    HoolokOutputConfig(id = "id_table_stream", kind = "stream", format = "csv", subtype = "table", mode = "append",
      trigger = Some("once"), options = Some(Map(
        "tableName" -> "stream_table",
        "gracefulShutdownPath" -> Files.createTempDirectory("hoolok_graceful_").toUri.toString,
        "gracefulShutdownFileNotCreated" -> "false",
        "gracefulShutdownDelay" -> "30"
      ))
    )
  val dataSourceStreamOutputConfig: HoolokOutputConfig =
    HoolokOutputConfig(id = "id_data_source_stream", kind = "stream", format = "csv", subtype = "data-source",
      mode = "append", trigger = Some("ProcessingTime(100)"), options = Some(Map(
        "inferSchema" -> "false",
        "gracefulShutdownPath" -> Files.createTempDirectory("hoolok_graceful_").toUri.toString,
        "gracefulShutdownFileNotCreated" -> "false",
        "gracefulShutdownDelay" -> "30"
      ))
    )
}
