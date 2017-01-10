package com.datalogs.utils

import org.apache.spark.sql._

/**
  * Created by joychak on 1/7/17.
  */
object CSVUtils {

  def loadAnyCSVAsTable(spark: SparkSession, path: String, tableName: String): DataFrame = {
    val data = spark
      .read
      .format("csv")
      .option("header", "true")
      //.option("inferSchema", "true")
      .option("parserLib", "UNIVOCITY") // <-- This is the configuration that solved the issue.
      .load(path)

    data.registerTempTable(tableName)
    data
  }

  def loadAnyCSVAsTable(spark: SparkSession, path: String): DataFrame = {
    loadAnyCSVAsTable(spark, path, inferTableNameFromPath(path))
  }

  private val pattern = "(\\w+)(\\.csv)?$".r.unanchored
  def inferTableNameFromPath(path: String) = path match {
    case pattern(filename, extension) => filename
    case _ => path
  }
}
