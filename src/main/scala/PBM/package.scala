package com.dreamteam.clickmodels

import org.apache.spark.sql.SparkSession

package object PBM {
  val spark: SparkSession = SparkSession.builder()
    .config("spark.jars", "/home/ultimatum/spark-jars/postgresql-42.2.6.jar")
    .master("local[*]").config("spark.executor.memory", "6g").getOrCreate()
}
