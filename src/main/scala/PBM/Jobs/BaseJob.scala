package com.dreamteam
package clickmodels
package PBM.Jobs

import com.dreamteam.clickmodels.PBM.Jobs.JobsArguments.{NoArgs, PBMJobArgs}

trait BaseJob[T <: PBMJobArgs] {
  def runJob(args: T): Unit
  def parseArgs(args: Array[String]): T = NoArgs

  def main(args: Array[String]): Unit = {
    val jobArgs: T = parseArgs(args)
    runJob(jobArgs)
  }
}
