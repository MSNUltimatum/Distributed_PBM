package com.dreamteam.clickmodels
package Spark.PBM.Jobs

import Spark.PBM.Jobs.JobsArguments.ReTrainArgs
import Spark.PBM.Model.PBMModelObj.PBMModel
import Spark.PBM.Util.DataClasses.PostgresConfigs

import pureconfig.generic.ProductHint
import pureconfig.generic.auto._
import pureconfig.{CamelCase, ConfigFieldMapping, ConfigObjectSource, ConfigSource}

object RetrainPostgresJob extends BaseJob[ReTrainArgs]{
  implicit def hint[PostgresConfigs]: ProductHint[PostgresConfigs] = ProductHint[PostgresConfigs](ConfigFieldMapping(CamelCase, CamelCase))
  lazy val source: ConfigObjectSource = ConfigSource.resources("configs/postgres-configs.json")

  override def runJob(args: ReTrainArgs): Unit = {
    val model: Either[PBMModel, String] = PBMModel.deserialize(args.pathToModel)
    model match {
      case Left(pbm) =>
        PBMModel.retrain.json(pbm, args.pathToData, args.iterationNums) match {
          case Left(retrained) =>
            source.load[PostgresConfigs] match {
              case Right(ps) => pbm.serialize.postgres(ps)
              case Left(value) => println(value)
            }
            if(args.modelSerializationPath.nonEmpty) retrained.serialize.parquet(args.modelSerializationPath)
          case Right(value) => println(value)
        }
      case Right(exp) => println(exp)
    }
  }
}
