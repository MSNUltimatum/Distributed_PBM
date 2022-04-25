package com.dreamteam.clickmodels
package PBM.Util

import java.util.Properties

private[PBM] object HelperClasses {
  trait AttractiveContainer {
    val attrNumerator: Double
    val attrDenominator: Float

    def attrValue: Double = attrNumerator / attrDenominator
  }

  trait ExamineContainer {
    val examNumerator: Double
    val examDenominator: Float

    def examValue: Double = examNumerator / examDenominator
  }

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
