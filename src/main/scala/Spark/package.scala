package com.dreamteam.clickmodels

import org.apache.spark.sql.SparkSession

import java.nio.file.Paths

package object Spark {
  val spark: SparkSession = SparkSession.builder()
    .config("spark.jars", Paths.get("src/main/resources/jars/postgresql-42.2.6.jar").toAbsolutePath.toString)
    .master("local[*]").config("spark.executor.memory", "6g").getOrCreate()
}
