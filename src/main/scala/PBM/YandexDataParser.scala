package com.dreamteam.clickmodels
package PBM

import PBM.Util.DataClasses.{TrainingSearchSession, TrainingWebResult}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

import java.util.UUID

object YandexDataParser {
  def parseUnstructuredYandexData(path: String): Dataset[TrainingSearchSession] = {
    import spark.implicits._
    val rdd: RDD[String] = spark.sparkContext.textFile(path)
    val parsedRdd: RDD[List[String]] = rdd.map(el => el.split("\t").toList)
    val aggregatedData: List[TrainingSearchSession] = parsedRdd.aggregate(List[TrainingSearchSession]())((prev, cur) => prepareSession(prev, cur), (l, r) => l ++ r)
    aggregatedData.toDS()
  }

  private def prepareSession(acc: List[TrainingSearchSession], currEl: List[String]): List[TrainingSearchSession] = (acc, currEl) match {
    case (lst, x) if x.contains("Q") => createSession(x) :: lst
    case (h :: t, x) if x.contains("C") => h.setClick(getWebResult(x(3), isClick = true)) :: t
    case _ => acc
  }

  private def createSession(value: List[String]): TrainingSearchSession = TrainingSearchSession(
    UUID.randomUUID().toString,
    value(3).toLong,
    value.head.toLong,
    value.drop(5).map(x => getWebResult(x))
  )

  private def getWebResult(str: String, isClick: Boolean = false): TrainingWebResult = TrainingWebResult(str.toLong, isClick)
}
