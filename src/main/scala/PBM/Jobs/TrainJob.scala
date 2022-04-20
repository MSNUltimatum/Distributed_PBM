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

  override def parseArgs(args: Array[String]): TrainArgs = {
    @tailrec
    def go(arr: List[String], acc: Map[String, String]): Map[String, String] = arr match {
      case Nil => acc
      case "--path_to_data" :: path :: t => go(t, acc + ("dataPath" -> path))
      case "--iterations_count" :: path :: t => go(t, acc + ("iterNum" -> path))
      case _ :: t => go(t, acc)
    }
    val map: Map[String, String] = go(args.toList, Map[String, String]())
    getParams(map)
  }

  private def getParams(map: Map[String, String]): TrainArgs = TrainArgs(
    map("dataPath"),
    Try(map("iterNum").toInt).getOrElse(20)
  )
}
