package com.dreamteam.clickmodels
package Spark.PBM.Jobs

object JobsArguments {
  sealed trait PBMJobArgs

  case object NoArgs extends PBMJobArgs

  case class TrainArgs(pathToData: String, iterationNums: Int, modelSerializationPath: String = "") extends PBMJobArgs

  case class ReTrainArgs(pathToData: String, pathToModel: String, iterationNums: Int, modelSerializationPath: String = "") extends PBMJobArgs
}
