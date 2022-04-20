package com.dreamteam.clickmodels
package PBM
package Model

import PBM.Estimator.EMEstimator
import PBM.Util.DataClasses._

import org.apache.spark.sql.functions.broadcast

import scala.reflect.runtime.universe.TypeTag
import org.apache.spark.sql.{Column, DataFrame, Dataset, SaveMode, functions}
import org.apache.spark.storage.StorageLevel

import scala.util.{Failure, Success, Try}

object PBMModelObj {
  case class PBMModel(modelDataset: Dataset[FullModelParameters]) {

    import spark.implicits._

    modelDataset.persist(StorageLevel.MEMORY_AND_DISK)

    private lazy val attractiveParameter: Dataset[AttractiveParameter] = getAttractivePart(modelDataset.toDF())
    private lazy val examParameters: Dataset[ExaminationParameter] = getExaminationPart(modelDataset.toDF())

    def serialize(pathToSerialize: String): Unit = {
      modelDataset.write.mode(SaveMode.Overwrite).parquet(pathToSerialize)
    }

    def getFullClickProbability[T <: SearchSession](session: T): Dataset[RankProbability] = {
      val probabilityColumn: Column = ($"attrNumerator" / $"attrDenominator") * ($"examNumerator" / $"examDenominator")
      val searchSessionDf: DataFrame = session.toExplodeSearchSession.toDF()
      val attrDf: DataFrame = addAttractive(searchSessionDf)
      val examDf: DataFrame = addExamination(attrDf)
      val filledDf: DataFrame = fillEmptyVals(examDf)
      filledDf.select($"query", $"result", $"rank", probabilityColumn.as("probability")).as[RankProbability]
    }

    def getConditionalClickProbability[T <: SearchSession](session: T): DataFrame = ???

    private def addAttractive(ds: DataFrame): DataFrame = broadcast(ds.as("ss"))
      .join(attractiveParameter.as("attr"), $"ss.query" === $"attr.query" && $"ss.result" === $"attr.resultId", "left")
      .drop($"ss.query")
      .drop($"attr.resultId")
      .drop($"attr.rank")

    private def addExamination(ds: DataFrame): DataFrame = broadcast(ds.as("ss"))
      .join(examParameters.as("ex"), $"ss.rank" === $"ex.rank", "left")
      .drop($"ex.rank")

    private def fillEmptyVals(df: DataFrame): DataFrame = df
      .na.fill(1, Seq("attrNumerator", "examNumerator"))
      .na.fill(2, Seq("attrDenominator", "examDenominator"))

    private def getAttractivePart(df: DataFrame): Dataset[AttractiveParameter] =
      df.groupBy("query", "resultId").agg(
        functions.first($"rank").as("rank"),
        functions.first($"pk").as("pk"),
        functions.first($"attrNumerator").as("attrNumerator"),
        functions.first($"attrDenominator").as("attrDenominator")).as[AttractiveParameter]

    private def getExaminationPart(df: DataFrame): Dataset[ExaminationParameter] =
      df.groupBy("rank").agg(functions.first($"examNumerator").as("examNumerator"),
        functions.first($"examDenominator").as("examDenominator")).as[ExaminationParameter]
  }

  object PBMModel {

    import spark.implicits._

    val estimator: EMEstimator = EMEstimator()

    def retrain[T <: TrainingSearchSession](pbm: PBMModel, ds: Dataset[T], iterNum: Int = 50): PBMModel =
      estimator.retrain(pbm, ds, iterNum)

    def deserialize(pathToModelFolder: String): Either[PBMModel, String] = {
      Try(spark.read.parquet(pathToModelFolder).as[FullModelParameters]) match {
        case Success(attr) => Left(PBMModel(attr))
        case Failure(exception) => Right(s"Cannot deserialize model parameters. Trace: ${exception.getMessage}")
      }
    }

    object train {
      def dataFrame[T <: TrainingSearchSession : TypeTag](df: DataFrame, iterNum: Int = 50): Either[PBMModel, String] =
        Try(df.as[T]) match {
          case Success(ds) => tryTrain(ds, iterNum)
          case Failure(exception) => Right(s"The error has occurred: ${exception.getMessage}")
        }

      def dataSet[T <: TrainingSearchSession](trainSessions: Dataset[T], iterNum: Int = 50): Either[PBMModel, String] =
        tryTrain(trainSessions, iterNum)

      def parquet[T <: TrainingSearchSession : TypeTag](path: String, iterNum: Int = 50): Either[PBMModel, String] = {
        val parquetDf: DataFrame = spark.read.parquet(path)
        dataFrame[T](parquetDf, iterNum)
      }

      private def tryTrain[T <: TrainingSearchSession](lazyDs: Dataset[T], iterNum: Int): Either[PBMModel, String] = Try(estimator.train(lazyDs, iterNum)) match {
        case Success(pbm) => Left(pbm)
        case Failure(exception) => Right(f"The error has occurred: ${exception.getMessage}.")
      }
    }
  }
}

