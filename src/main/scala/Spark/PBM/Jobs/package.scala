package com.dreamteam.clickmodels
package Spark.PBM

import Spark.PBM.Jobs.JobsArguments.{PBMJobArgs, TrainArgs}

import scala.annotation.tailrec
import scala.util.Try

package object Jobs {
  trait ArgsImplicit[T <: PBMJobArgs] {
    def parseArgs(args: Array[String]): T
  }

  implicit val implTrainArgs: ArgsImplicit[TrainArgs] = new ArgsImplicit[TrainArgs] {
    override def parseArgs(args: Array[String]): TrainArgs = {
      @tailrec
      def go(arr: List[String], acc: Map[String, String]): Map[String, String] = arr match {
        case Nil => acc
        case "--path_to_data" :: path :: t => go(t, acc + ("dataPath" -> path))
        case "--iterations_count" :: path :: t => go(t, acc + ("iterNum" -> path))
        case "--serialize_model" :: path :: t => go(t, acc + ("serModelPath" -> path))
        case _ :: t => go(t, acc)
      }

      val map: Map[String, String] = go(args.toList, Map[String, String]())
      getParams(map)
    }

    private def getParams(map: Map[String, String]): TrainArgs = TrainArgs(
      map("dataPath"),
      Try(map("iterNum").toInt).getOrElse(20),
      map.getOrElse("serModelPath", "")
    )
  }
}
