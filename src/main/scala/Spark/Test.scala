package com.dreamteam.clickmodels
package Spark
import Compaction.Logic.Compaction
import com.dreamteam.clickmodels.Spark.PBM.Util.DataClasses.{TrainingSearchSession, TrainingWebResult}
import org.apache.spark.sql.functions

object Test extends App {
  import spark.implicits._
  val df = spark.read.json("hdfs://172.27.1.5:8020/output/pbm_results")
  println(df.as[TrainingSearchSession].filter(ss => l(ss.results)).count())
  def l(lst: List[TrainingWebResult]): Boolean = {
    val res = lst.map(s => s.resultId)
    res.zip(res.tail).exists {case (x,y) => x > y}
  }
//  val a = Compaction.CompactedDf.json("hdfs://172.27.1.5:8020/output/pbm_results", 40)
//  a match {
//    case Left(value) => value.serialize.json("hdfs://172.27.1.5:8020/compacted/")
//    case Right(value) => println(value)
//  }
}
