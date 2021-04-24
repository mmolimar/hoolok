package com.github.mmolimar.hoolok

import com.amazon.deequ.analyzers.Size

package object steps {

  val dfIdName = "test"
  val showConfig: HoolokStepConfig = HoolokStepConfig(id = dfIdName, kind = "show", options = None, dq = None)
  val cacheConfig: HoolokStepConfig = HoolokStepConfig(id = dfIdName, kind = "cache", options = None, dq = None)
  val unpersistConfig: HoolokStepConfig = HoolokStepConfig(id = dfIdName, kind = "unpersist", options = None, dq = None)
  val checkpointConfig: HoolokStepConfig = HoolokStepConfig(id = dfIdName, kind = "checkpoint", options = None, dq = None)
  val fromJsonConfig: HoolokStepConfig = {
    val options = Map(
      "dataframe" -> dfIdName,
      "columns" -> "col2,col3",
      "alias" -> "alias_col2,alias_col3",
      "schema" -> "schema-test-json",
      "select" -> "alias_col2.*"
    )
    HoolokStepConfig(id = "test_json", kind = "from-json", options = Some(options), dq = None)
  }
  val fromAvroConfig: HoolokStepConfig = {
    val options = Map(
      "dataframe" -> dfIdName,
      "columns" -> "col4,col5",
      "alias" -> "alias_col4,alias_col5",
      "schema" -> "schema-test-avro",
      "select" -> "alias_col4.*"
    )
    HoolokStepConfig(id = "test_avro", kind = "from-avro", options = Some(options), dq = None)
  }
  val toAvroConfig: HoolokStepConfig = {
    val options = Map(
      "dataframe" -> dfIdName,
      "columns" -> "col2,col3",
      "alias" -> "alias_col2, alias_col3",
      "schema" -> "schema-test",
      "select" -> "alias_col2, alias_col3"
    )
    HoolokStepConfig(id = "test_avro", kind = "to-avro", options = Some(options), dq = None)
  }
  val sqlConfig: HoolokStepConfig = {
    val options = Map("query" -> "select col1, col2, col6 from test")
    val dq = HoolokDataQualityConfig(
      analysis = Some(HoolokDataQualityAnalysisConfig(
        name = "test_analysis",
        analyzers = HoolokDataQualityAnalyzerConfig(size = Some(Size()))
      )),
      verification = Some(HoolokDataQualityVerificationConfig(
        name = "test_verification",
        checks = List(
          HoolokDataQualityCheckConfig(
            level = "error",
            description = "desc1",
            isUnique = Some(HoolokDataQualityCheckIsUniqueConfig(column = "col1"))
          ),
          HoolokDataQualityCheckConfig(
            level = "warning",
            description = "desc2",
            hasSize = Some(HoolokDataQualityCheckHasSizeConfig(op = "<=", value = 2)),
            containsEmail = Some(HoolokDataQualityCheckContainsEmailConfig(column = "col2"))
          )
        )
      ))
    )
    HoolokStepConfig(id = "test_sql", kind = "sql", options = Some(options), dq = Some(dq))
  }

}
