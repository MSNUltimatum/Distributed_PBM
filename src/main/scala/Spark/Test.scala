package com.dreamteam.clickmodels
package Spark

import Spark.Compaction.Compaction

object Test extends App {
  val a = Compaction.CompactedDf.json("hdfs://172.27.1.5:8020/output/pbm_results", 40)
  a match {
    case Left(value) => value.serialize.json("hdfs://172.27.1.5:8020/compacted/")
    case Right(value) => println(value)
  }
}
