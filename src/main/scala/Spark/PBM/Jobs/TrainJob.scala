package com.dreamteam.clickmodels
package Spark.PBM.Jobs

import Spark.PBM.Jobs.JobsArguments.TrainArgs
import Spark.PBM.Model.PBMModelObj.PBMModel

object TrainJob extends BaseJob[TrainArgs] {
  override def runJob(args: TrainArgs): Unit = {
    val model: Either[PBMModel, String] = PBMModel.train.parquet(args.pathToData, args.iterationNums)
    model match {
      case Left(_) => println("Success")
      case Right(exp) => println(s"Error: $exp")
    }
  }
}
