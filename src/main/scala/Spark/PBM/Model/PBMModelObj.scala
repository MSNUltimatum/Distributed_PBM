package com.dreamteam.clickmodels
package Spark
package PBM.Model

import Spark.PBM.Estimator.EMEstimator
import Spark.PBM.Util.DataClasses.{AttractiveParameter, ExaminationParameter, FullModelParameters, PostgresConfigs, TrainingSearchSession}

import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

import scala.reflect.runtime.universe.TypeTag
import scala.util.{Failure, Success, Try}

object PBMModelObj {
  case class PBMModel(modelDataset: Dataset[FullModelParameters]) {

    import spark.implicits._

    modelDataset.persist(StorageLevel.MEMORY_AND_DISK)

    private lazy val attractiveParameter: Dataset[AttractiveParameter] = getAttractivePart(modelDataset.toDF())
    private lazy val examParameters: Dataset[ExaminationParameter] = getExaminationPart(modelDataset.toDF())

    object serialize {
      def parquet(pathToSerialize: String): Unit = modelDataset.write.mode(SaveMode.Overwrite).parquet(pathToSerialize)

      def postgres(postgresConfigs: PostgresConfigs): Unit = {
        attractiveParameter
          .write
          .option("truncate", "true")
          .mode(SaveMode.Overwrite)
          .jdbc(postgresConfigs.connectionPath, postgresConfigs.attractiveTable, postgresConfigs.getConnProperties)

        examParameters
          .write
          .option("truncate", "true")
          .mode(SaveMode.Overwrite)
          .jdbc(postgresConfigs.connectionPath, postgresConfigs.examinationTable, postgresConfigs.getConnProperties)
      }
    }

    private def getAttractivePart(df: DataFrame): Dataset[AttractiveParameter] =
      df.groupBy("query", "resultId").agg(
        functions.first($"attrNumerator").as("attrNumerator"),
        functions.first($"attrDenominator").as("attrDenominator")).as[AttractiveParameter]

    private def getExaminationPart(df: DataFrame): Dataset[ExaminationParameter] =
      df.groupBy("rank").agg(functions.first($"examNumerator").as("examNumerator"),
        functions.first($"examDenominator").as("examDenominator")).as[ExaminationParameter]
  }

  object PBMModel {

    import spark.implicits._

    val estimator: EMEstimator = EMEstimator()

    def deserialize(pathToModelFolder: String): Either[PBMModel, String] = {
      Try(spark.read.parquet(pathToModelFolder).as[FullModelParameters]) match {
        case Success(attr) => Left(PBMModel(attr))
        case Failure(exception) => Right(s"Cannot deserialize model parameters. Trace: ${exception.getMessage}")
      }
    }

    object retrain {
      def json[T <: TrainingSearchSession : TypeTag](pbm: PBMModel, path: String, iterNum: Int = 50): Either[PBMModel, String] = {
        val df: DataFrame = spark.read.json(path)
        dataFrame[T](pbm, df, iterNum)
      }

      def dataFrame[T <: TrainingSearchSession : TypeTag](pbm: PBMModel, df: DataFrame, iterNum: Int = 50): Either[PBMModel, String] =
        Try(df.as[T]) match {
          case Success(ds) => tryReTrain(pbm, ds, iterNum)
          case Failure(exception) => Right(s"The error has occurred: ${exception.getMessage}")
        }

      private def tryReTrain[T <: TrainingSearchSession](pbm: PBMModel, lazyDs: Dataset[T], iterNum: Int): Either[PBMModel, String] =
        Try(estimator.retrain(pbm, lazyDs, iterNum)) match {
          case Success(pbm) => Left(pbm)
          case Failure(exception) => Right(f"The error has occurred: ${exception.getMessage}.")
        }
    }

    object train {
      def parquet[T <: TrainingSearchSession : TypeTag](path: String, iterNum: Int = 50): Either[PBMModel, String] =
        tryReadAndTrain[T](spark.read.parquet(path), iterNum)

      def json[T <: TrainingSearchSession : TypeTag](path: String, iterNum: Int = 50): Either[PBMModel, String] =
        tryReadAndTrain[T](spark.read.json(path), iterNum)

      private def tryReadAndTrain[T <: TrainingSearchSession : TypeTag](option: => DataFrame, iterNum: Int): Either[PBMModel, String] =
        Try(option) match {
          case Success(df) => dataFrame[T](df, iterNum)
          case Failure(ex) => Right(ex.getMessage)
        }

      def dataSet[T <: TrainingSearchSession](trainSessions: Dataset[T], iterNum: Int = 50): Either[PBMModel, String] =
        tryTrain(trainSessions, iterNum)

      def dataFrame[T <: TrainingSearchSession : TypeTag](df: DataFrame, iterNum: Int = 50): Either[PBMModel, String] =
        Try(df.as[T]) match {
          case Success(ds) => tryTrain(ds, iterNum)
          case Failure(exception) => Right(s"The error has occurred: ${exception.getMessage}")
        }

      private def tryTrain[T <: TrainingSearchSession](lazyDs: Dataset[T], iterNum: Int): Either[PBMModel, String] =
        Try(estimator.train(lazyDs, iterNum)) match {
          case Success(pbm) => Left(pbm)
          case Failure(exception) => Right(f"The error has occurred: ${exception.getMessage}.")
        }
    }
  }
}

