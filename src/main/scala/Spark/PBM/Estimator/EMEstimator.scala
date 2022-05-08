package com.dreamteam.clickmodels
package Spark
package PBM.Estimator

import Spark.PBM.Model.PBMModelObj.PBMModel
import Spark.PBM.Util.DataClasses.{FullModelParameters, TrainingSearchSession}

import org.apache.spark.sql.expressions.{UserDefinedFunction, Window, WindowSpec}
import org.apache.spark.sql.functions.{explode, lit, row_number, udf, when}
import org.apache.spark.sql.{Column, DataFrame, Dataset, functions}

import scala.annotation.tailrec

private[PBM] case class EMEstimator() {
  import spark.implicits._

  private val uuid: UserDefinedFunction = udf(() => java.util.UUID.randomUUID().toString)

  private val attrUpdateColumn: Column = (lit(1) - ($"train.examNumerator" / $"train.examDenominator")) * ($"train.attrNumerator" / $"train.attrDenominator") /
    (lit(1) - ($"train.examNumerator" / $"train.examDenominator") * ($"train.attrNumerator" / $"train.attrDenominator"))

  private val examUpdateColumn: Column = (lit(1) - ($"train.attrNumerator" / $"train.attrDenominator")) * ($"train.examNumerator" / $"train.examDenominator") /
    (lit(1) - ($"train.examNumerator" / $"train.examDenominator") * ($"train.attrNumerator" / $"train.attrDenominator"))

  def train[T <: TrainingSearchSession](trainSessions: Dataset[T], iterNum: Int): PBMModel = {
    require(iterNum > 1)
    val explodedDf: DataFrame = explodeResults(trainSessions.toDF())
    val emPrepared: DataFrame = addStatisticsColumn(explodedDf)
    val estimatedModelDS: DataFrame = iterativelyEstimate(iterNum, emPrepared)
    PBMModel(modelDataset = estimatedModelDS.as[FullModelParameters])
  }

  def retrain[T <: TrainingSearchSession](pbm: PBMModel, ds: Dataset[T], iterNum: Int): PBMModel = {
    require(iterNum > 1)
    val currentDf: DataFrame = pbm.modelDataset.toDF()
    val depletedDf: DataFrame = depleteDf(currentDf)
    val explodedDf: DataFrame = explodeResults(ds.toDF())
    val emPrepared: DataFrame = addStatisticsColumn(explodedDf).drop($"result")
    val retrained: DataFrame = iterativelyEstimate(iterNum, depletedDf.union(emPrepared))
    PBMModel(modelDataset = retrained.as[FullModelParameters])
  }

  private def depleteDf(frame: DataFrame): DataFrame = frame
    .withColumn("attrNumerator", $"attrNumerator" * lit(0.95))
    .withColumn("examNumerator", $"attrNumerator" * lit(0.95))

  private def explodeResults(trainSessions: DataFrame): DataFrame = {
    val windowSpec: WindowSpec = Window.partitionBy($"recordId").orderBy(lit(1))
    val rankColumn: Column = row_number().over(windowSpec)
    trainSessions
      .withColumn("recordId", uuid())
      .select($"recordId", $"query", rankColumn.as("rank"), explode($"results").as("result"))
      .withColumn("resultId", $"result.resultId")
      .withColumn("clicked", $"result.clicked")
  }

  private def addStatisticsColumn(df: DataFrame): DataFrame = df
    .withColumn("attrNumerator", lit(1))
    .withColumn("attrDenominator", lit(2))
    .withColumn("examNumerator", lit(1))
    .withColumn("examDenominator", lit(2))

  private def iterativelyEstimate(iterNum: Int, originModel: DataFrame): DataFrame = {
    @tailrec
    def rec(iter: Int, df: DataFrame): DataFrame = iter match {
      case i if i > 0 && i % 10 == 0 => df.cache(); rec(i - 1, update(df))
      case i if i > 0 => rec(i - 1, update(df))
      case _ => df
    }

    rec(iterNum, originModel)
  }

  private def update(startDS: DataFrame): DataFrame = {
    val updatedDf: DataFrame = startDS.as("train")
      .select(
        $"train.recordId",
        $"train.query".as("query"),
        $"train.resultId".as("resultId"),
        $"train.rank",
        $"train.clicked",
        when($"train.clicked" === true, lit(1)).otherwise(attrUpdateColumn).as("attrNumerator"),
        lit(1).as("attrDenominator"),
        when($"train.clicked" === true, lit(1)).otherwise(examUpdateColumn).as("examNumerator"),
        lit(1).as("examDenominator"),
      )
    sumAcrossDataFrame(updatedDf)
  }

  private def sumAcrossDataFrame(frame: DataFrame): DataFrame = frame
    .withColumn("attrNumerator", functions.sum("attrNumerator").over(Window.partitionBy("query", "resultId")) + lit(1))
    .withColumn("attrDenominator", functions.sum("attrDenominator").over(Window.partitionBy("query", "resultId")) + lit(2))
    .withColumn("examNumerator", functions.sum("examNumerator").over(Window.partitionBy("rank")) + lit(1))
    .withColumn("examDenominator", functions.sum("examDenominator").over(Window.partitionBy("rank")) + lit(2))
}
