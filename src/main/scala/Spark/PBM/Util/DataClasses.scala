package com.dreamteam.clickmodels
package Spark.PBM.Util

import java.time.LocalDateTime
import java.util.Properties

object DataClasses {
  case class TrainingWebResult(resultId: Long, clicked: Boolean)

  case class TrainingSearchSession(query: Long, timestamp: String, results: List[TrainingWebResult]) {
    def setClick(result: TrainingWebResult): TrainingSearchSession = TrainingSearchSession(
      query,
      timestamp,
      results.map(f => if (f.resultId == result.resultId) result else f)
    )
  }

  case class AttractiveParameter(query: Long, resultId: Long, attrNumerator: Double, attrDenominator: Long)

  case class ExaminationParameter(rank: Long, examNumerator: Double, examDenominator: Long)

  case class FullModelParameters(recordId: String,
                                 query: Long,
                                 rank: Long,
                                 resultId: Long,
                                 attrNumerator: Double,
                                 attrDenominator: Long,
                                 examNumerator: Double,
                                 examDenominator: Long,
                                 clicked: Boolean)

  case class PostgresConfigs(attractiveTable: String,
                             examinationTable: String,
                             connectionPath: String,
                             user: String,
                             password: String,
                             driver: String = "org.postgresql.Driver"){
    def getConnProperties: Properties = {
      val connectionProperties = new Properties()
      connectionProperties.put("user", user)
      connectionProperties.put("password", password)
      connectionProperties.put("driver", driver)
      connectionProperties
    }
  }
}
