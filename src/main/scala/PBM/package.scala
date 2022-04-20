package com.dreamteam.clickmodels

import org.apache.spark.sql.SparkSession

package object PBM {
  val spark: SparkSession = SparkSession.builder().master("local[*]").config("spark.executor.memory", "6g").getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
}
