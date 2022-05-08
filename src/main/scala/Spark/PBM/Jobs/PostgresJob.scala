package com.dreamteam.clickmodels
package Spark.PBM.Jobs

import Spark.PBM.Jobs.JobsArguments.TrainArgs
import Spark.PBM.Model.PBMModelObj.PBMModel
import Spark.PBM.Util.DataClasses.{PostgresConfigs, TrainingSearchSession}

import pureconfig.generic.ProductHint
import pureconfig.generic.auto._
import pureconfig.{CamelCase, ConfigFieldMapping, ConfigObjectSource, ConfigSource}

object PostgresJob extends BaseJob[TrainArgs] {
  implicit def hint[PostgresConfigs]: ProductHint[PostgresConfigs] = ProductHint[PostgresConfigs](ConfigFieldMapping(CamelCase, CamelCase))
  lazy val source: ConfigObjectSource = ConfigSource.resources("configs/postgres-configs.json")

  override def runJob(args: TrainArgs): Unit = {
    val model: Either[PBMModel, String] = PBMModel.train.json[TrainingSearchSession](args.pathToData, args.iterationNums)
    model match {
      case Left(pbm) =>
        source.load[PostgresConfigs] match {
          case Right(ps) => pbm.serialize.postgres(ps)
          case Left(value) => println(value)
        }
        if(args.modelSerializationPath.nonEmpty) pbm.serialize.parquet(args.modelSerializationPath)
      case Right(exp) => println(exp)
    }
  }
}
