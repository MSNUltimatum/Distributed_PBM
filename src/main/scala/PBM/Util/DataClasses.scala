package com.dreamteam.clickmodels
package PBM.Util

object DataClasses {
  trait SearchResult[T] {
    val resultId: T
  }

  trait SearchSessionTrait[Q, T <: SearchResult[Long]] {
    val query: Q
    val results: List[T]

    def toExplodeSearchSession: List[ExplodeSession[Q, Long]] = results.zipWithIndex map {
      case (res, ind) => ExplodeSession(query, res.resultId, ind + 1)
    }
  }


  case class WebResult(resultId: Long) extends SearchResult[Long]

  case class TrainingWebResult(resultId: Long, clicked: Boolean)

  case class SearchSession(query: Long, results: List[WebResult]) extends SearchSessionTrait[Long, WebResult]

  case class TrainingSearchSession(pk: String, query: Long, sessionId: Long, results: List[TrainingWebResult]) {
    def setClick(result: TrainingWebResult): TrainingSearchSession = TrainingSearchSession(
      pk,
      query,
      sessionId,
      results.map(f => if (f.resultId == result.resultId) result else f)
    )
  }

  case class ExplodeSession[Q, T](query: Q, result: T, rank: Int)

  case class RankProbability(query: Long, result: Long, rank: Long, probability: Double)

  case class AttractiveParameter(query: Long, resultId: Long, rank: Long, attrNumerator: Double, attrDenominator: Long)

  case class ExaminationParameter(rank: Long, examNumerator: Double, examDenominator: Long)

  case class FullModelParameters(pk: String,
                                 query: Long,
                                 rank: Long,
                                 resultId: Long,
                                 attrNumerator: Double,
                                 attrDenominator: Long,
                                 examNumerator: Double,
                                 examDenominator: Long,
                                 clicked: Boolean)
}
