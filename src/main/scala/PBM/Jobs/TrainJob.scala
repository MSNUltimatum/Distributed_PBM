package com.dreamteam.clickmodels
package PBM
package Jobs

import PBM.Jobs.JobsArguments.TrainArgs
import PBM.Model.PBMModelObj.PBMModel

import scala.annotation.tailrec
import scala.util.Try

object TrainJob extends BaseJob[TrainArgs] {
  override def runJob(args: TrainArgs): Unit = {
    val model: Either[PBMModel, String] = PBMModel.train.parquet(args.pathToData, args.iterationNums)
    model match {
      case Left(_) => println("Success")
      case Right(exp) => println(s"Error: $exp")
    }
  }
}
