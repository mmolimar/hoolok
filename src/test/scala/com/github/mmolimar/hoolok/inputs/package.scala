package com.github.mmolimar.hoolok

package object inputs {

  val tableBatchInputConfig: HoolokInputConfig =
    HoolokInputConfig(id = "id_table_batch", kind = "batch", format = "csv", subtype = "table", options = Some(Map(
      "tableName" -> "batch_table"
    )))
  val dataSourceBatchInputConfig: HoolokInputConfig =
    HoolokInputConfig(id = "id_data_source_batch", kind = "batch", format = "csv", subtype = "data-source",
      schema = Some("schema-test"), options = Some(Map(
        "inferSchema" -> "false"
      ))
    )
  val tableStreamInputConfig: HoolokInputConfig =
    HoolokInputConfig(id = "id_table_stream", kind = "stream", format = "csv", subtype = "table", options = Some(Map(
      "tableName" -> "stream_table"
    )))
  val dataSourceStreamInputConfig: HoolokInputConfig =
    HoolokInputConfig(id = "id_data_source_stream", kind = "stream", format = "csv", subtype = "data-source",
      schema = Some("schema-test"), options = Some(Map(
        "inferSchema" -> "false"
      ))
    )

}
