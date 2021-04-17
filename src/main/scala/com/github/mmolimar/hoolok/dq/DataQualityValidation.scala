package com.github.mmolimar.hoolok.dq

import org.apache.spark.sql.DataFrame

trait DataQualityValidation {

  def applyDataQuality(kind: String, id: String, dataframe: DataFrame): Unit

}
