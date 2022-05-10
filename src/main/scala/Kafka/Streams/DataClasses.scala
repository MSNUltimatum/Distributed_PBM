package com.dreamteam.clickmodels
package Kafka.Streams

import io.circe.parser._
import io.circe.syntax._
import io.circe.{Decoder, Encoder, HCursor, Json}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.serialization.Serdes.fromFn

object DataClasses {
  case class ResultWithClick(resultId: Long, clicked: Boolean)

  case class RankedClick(rank: Long, clicked: Boolean)

  case class RankCount(rank: Long, count: Long)

  case class QueryResults(query: Long, timestamp: String, results: List[ResultWithClick])

  object implicits {
    implicit val encodeResult: Encoder[ResultWithClick] = (a: ResultWithClick) => Json.obj(
      ("resultId", Json.fromLong(a.resultId)),
      ("clicked", Json.fromBoolean(a.clicked))
    )

    implicit val encodeRanked: Encoder[RankedClick] = (a: RankedClick) => Json.obj(
      ("rank", Json.fromLong(a.rank)),
      ("clicked", Json.fromBoolean(a.clicked))
    )

    implicit val encodeRankedCount: Encoder[RankCount] = (a: RankCount) => Json.obj(
      ("rank", Json.fromLong(a.rank)),
      ("count", Json.fromLong(a.count))
    )

    implicit val decodeResult: Decoder[ResultWithClick] = (c: HCursor) => for {
      resultId <- c.downField("resultId").as[Long]
      clicked <- c.downField("clicked").as[Boolean]
    } yield {
      ResultWithClick(resultId, clicked)
    }

    implicit val decodeRanked: Decoder[RankedClick] = (c: HCursor) => for {
      rank <- c.downField("rank").as[Long]
      clicked <- c.downField("clicked").as[Boolean]
    } yield {
      RankedClick(rank, clicked)
    }

    implicit val decodeRankCount: Decoder[RankCount] = (c: HCursor) => for {
      rank <- c.downField("rank").as[Long]
      count <- c.downField("count").as[Long]
    } yield {
      RankCount(rank, count)
    }

    implicit val decodeQueryResult: Decoder[QueryResults] = (c: HCursor) => for {
      query <- c.downField("query").as[Long]
      ts <- c.downField("timestamp").as[String]
      res <- c.downField("results").as[List[ResultWithClick]]
    } yield {
      QueryResults(query, ts, res)
    }

    implicit val serdeResult: Serde[RankedClick] = {
      val ser: RankedClick => Array[Byte] = (n: RankedClick) => n.asJson.noSpaces.getBytes
      val deserializer: Array[Byte] => Option[RankedClick] = (a: Array[Byte]) => {
        val str = new String(a)
        decode[RankedClick](str).toOption
      }
      fromFn(ser, deserializer)
    }

    implicit val serdeNC: Serde[RankCount] = {
      val ser: RankCount => Array[Byte] = (n: RankCount) => n.asJson.noSpaces.getBytes
      val deserializer: Array[Byte] => Option[RankCount] = (a: Array[Byte]) => {
        val str = new String(a)
        decode[RankCount](str).toOption
      }
      fromFn(ser, deserializer)
    }
  }
}
