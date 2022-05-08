package com.dreamteam.clickmodels
package Spark.PBM

import Spark.PBM.Jobs.JobsArguments.{PBMJobArgs, ReTrainArgs, TrainArgs}

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
        case "--path_to_data" :: path :: t => go(t, acc + ("path_to_data" -> path))
        case "--iterations_count" :: path :: t => go(t, acc + ("iterations_count" -> path))
        case "--serialize_model" :: path :: t => go(t, acc + ("serialize_model" -> path))
        case _ :: t => go(t, acc)
      }

      val map: Map[String, String] = go(args.toList, Map[String, String]())
      getParams(map)
    }

    private def getParams(map: Map[String, String]): TrainArgs = TrainArgs(
      map("path_to_data"),
      Try(map("iterations_count").toInt).getOrElse(20),
      map.getOrElse("serialize_model", "")
    )
  }

  implicit val implReTrainArgs: ArgsImplicit[ReTrainArgs] = new ArgsImplicit[ReTrainArgs] {
    override def parseArgs(args: Array[String]): ReTrainArgs = {
      @tailrec
      def go(arr: List[String], acc: Map[String, String]): Map[String, String] = arr match {
        case Nil => acc
        case "--path_to_data" :: path :: t => go(t, acc + ("path_to_data" -> path))
        case "--path_to_model" :: path :: t => go(t, acc + ("path_to_model" -> path))
        case "--iterations_count" :: count :: t => go(t, acc + ("iterations_count" -> count))
        case "--serialize_model" :: path :: t => go(t, acc + ("serialize_model" -> path))
        case _ :: t => go(t, acc)
      }

      val map: Map[String, String] = go(args.toList, Map[String, String]())
      getParams(map)
    }

    private def getParams(map: Map[String, String]): ReTrainArgs = ReTrainArgs(
      map("path_to_data"),
      map("path_to_model"),
      Try(map("iterations_count").toInt).getOrElse(20),
      map.getOrElse("serialize_model", "")
    )
  }

}
