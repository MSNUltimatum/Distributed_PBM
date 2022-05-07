package com.dreamteam.clickmodels
package Spark.PBM.Jobs

import JobsArguments.PBMJobArgs

abstract class BaseJob[T <: PBMJobArgs : ArgsImplicit]() {
  def runJob(args: T): Unit

  def main(args: Array[String]): Unit = {
    val jobArgs: T = implicitly[ArgsImplicit[T]].parseArgs(args)
    runJob(jobArgs)
  }
}
