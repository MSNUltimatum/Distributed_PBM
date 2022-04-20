package com.dreamteam.clickmodels
package PBM.Util

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


}
