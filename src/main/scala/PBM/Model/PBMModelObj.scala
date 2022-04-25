package com.dreamteam.clickmodels
package PBM
package Model

import PBM.Estimator.EMEstimator
import PBM.Util.DataClasses._
import PBM.Util.HelperClasses.PostgresConfigs

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
        functions.first($"rank").as("rank"),
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

      def json[T <: TrainingSearchSession : TypeTag](path: String, iterNum: Int = 50): Either[PBMModel, String] = {
        val jsonDf: DataFrame = spark.read.json(path)
        dataFrame[T](jsonDf, iterNum)
      }

      private def tryTrain[T <: TrainingSearchSession](lazyDs: Dataset[T], iterNum: Int): Either[PBMModel, String] = Try(estimator.train(lazyDs, iterNum)) match {
        case Success(pbm) => Left(pbm)
        case Failure(exception) => Right(f"The error has occurred: ${exception.getMessage}.")
      }
    }
  }
}

