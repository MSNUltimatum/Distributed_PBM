package com.dreamteam.clickmodels
package PBM

import PBM.Model.PBMModelObj.PBMModel
import PBM.Util.DataClasses.{SearchSession, TrainingSearchSession, WebResult}
import org.apache.spark.sql.Dataset

object TT1 extends App {

  import spark.implicits._

  val path: String = "/Users/esaraev/Documents/Study/DistributedPBMModel/src/main/resources/YandexData/TextFile/YandexRelPredChallenge"
  val startDf: Dataset[TrainingSearchSession] = YandexDataParser.parseUnstructuredYandexData(path)
  //      startDf.write.mode(SaveMode.Overwrite).parquet("/Users/esaraev/Documents/Study/DistributedPBMModel/src/main/resources/YandexData/parquet")
//  val model: Either[PBMModel, String] = PBMModel.train.parquet[TrainingSearchSession]("/Users/esaraev/Documents/Study/DistributedPBMModel/src/main/resources/YandexData/parquetLarge/", 20)
  val model: Either[PBMModel, String] = PBMModel.deserialize("/Users/esaraev/Documents/Study/DistributedPBMModel/src/main/resources/serializedPBM")
  val retrainedModel = model match {
    //    case Left(pbm) => pbm.attrParameters.show(false)
    case Left(pbm) => PBMModel.retrain(pbm, startDf, 10)
    //    case Left(pbm) => pbm.getFullClickProbability(SearchSession(8, List(7, 103, 51, 92, 43, 12, 73, 69, 27, 105).map(x => WebResult(x)))).orderBy(functions.desc("probability")).show(false)
    //    case Left(pbm) => pbm.serialize("/Users/esaraev/Documents/Study/DistributedPBMModel/src/main/resources/serializedPBM")
    case _ => throw new Exception()
  }
  retrainedModel.getFullClickProbability(SearchSession(8, List(7, 103, 51, 92, 43, 12, 73, 69, 27, 105).map(x => WebResult(x)))).show(false)
}
