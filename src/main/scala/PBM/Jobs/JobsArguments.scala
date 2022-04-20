package com.dreamteam.clickmodels
package PBM.Jobs


object JobsArguments {
  trait PBMJobArgs
  case object NoArgs extends PBMJobArgs
  case class TrainArgs(pathToData: String, iterationNums: Int) extends PBMJobArgs
}
