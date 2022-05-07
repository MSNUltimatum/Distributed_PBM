package com.dreamteam.clickmodels
package Spark
package PBM

import PBM.Model.PBMModelObj.PBMModel
import com.dreamteam.clickmodels.Spark.PBM.Util.DataClasses.PostgresConfigs
import pureconfig._
import pureconfig.generic.ProductHint
import pureconfig.generic.auto._

object TT1 extends App {

  import spark.implicits._

  implicit def hint[PostgresConfigs]: ProductHint[PostgresConfigs] = ProductHint[PostgresConfigs](ConfigFieldMapping(CamelCase, CamelCase))
  val source = ConfigSource.resources("configs/postgres-configs.json")

//  val path: String = "/Users/esaraev/Documents/Study/DistributedPBMModel/src/main/resources/YandexData/TextFile/YandexRelPredChallenge"
//  val startDf: Dataset[TrainingSearchSession] = YandexDataParser.parseUnstructuredYandexData(path)
  //      startDf.write.mode(SaveMode.Overwrite).parquet("/Users/esaraev/Documents/Study/DistributedPBMModel/src/main/resources/YandexData/parquet")
//  val model: Either[PBMModel, String] = PBMModel.train.parquet[TrainingSearchSession]("/Users/esaraev/Documents/Study/DistributedPBMModel/src/main/resources/YandexData/parquetLarge/", 20)
  val model: Either[PBMModel, String] = PBMModel.deserialize("/home/ultimatum/Документы/Study/Distributed_PBM/src/main/resources/serializedPBM")
  model match {
    //    case Left(pbm) => pbm.attrParameters.show(false)
    case Left(pbm) => source.load[PostgresConfigs] match {
      case Right(ps) => pbm.serialize.postgres(ps)
      case Left(value) => println(value)
    }
    //    case Left(pbm) => pbm.getFullClickProbability(SearchSession(8, List(7, 103, 51, 92, 43, 12, 73, 69, 27, 105).map(x => WebResult(x)))).orderBy(functions.desc("probability")).show(false)
    //    case Left(pbm) => pbm.serialize("/Users/esaraev/Documents/Study/DistributedPBMModel/src/main/resources/serializedPBM")
    case Right(ex) => println(ex)
  }
//  retrainedModel.getFullClickProbability(SearchSession(8, List(7, 103, 51, 92, 43, 12, 73, 69, 27, 105).map(x => WebResult(x)))).show(false)
}
