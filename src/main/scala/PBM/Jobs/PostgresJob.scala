package com.dreamteam.clickmodels
package PBM.Jobs

import PBM.Jobs.JobsArguments.TrainArgs
import PBM.Model.PBMModelObj.PBMModel
import PBM.Util.HelperClasses.PostgresConfigs

import pureconfig.generic.ProductHint
import pureconfig.generic.auto._
import pureconfig.{CamelCase, ConfigFieldMapping, ConfigObjectSource, ConfigSource}

object PostgresJob extends BaseJob[TrainArgs] {
  implicit def hint[PostgresConfigs]: ProductHint[PostgresConfigs] = ProductHint[PostgresConfigs](ConfigFieldMapping(CamelCase, CamelCase))
  lazy val source: ConfigObjectSource = ConfigSource.resources("configs/postgres-configs.json")

  override def runJob(args: TrainArgs): Unit = {
    val model: Either[PBMModel, String] = PBMModel.train.parquet(args.pathToData, args.iterationNums)
    model match {
      case Left(pbm) => source.load[PostgresConfigs] match {
        case Right(ps) => pbm.serialize.postgres(ps)
        case Left(value) => println(value)
      }
      case Right(exp) => println(s"Error: $exp")
    }
  }
}
