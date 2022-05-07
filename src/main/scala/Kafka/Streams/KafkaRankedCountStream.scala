package com.dreamteam.clickmodels
package Kafka.Streams

import Kafka.Streams.DataClasses.implicits._
import Kafka.Streams.DataClasses.{QueryResults, RankCount, RankedClick}

import io.circe.Json
import io.circe.parser._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.Named
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

import java.time.Duration
import java.util.Properties

object KafkaRankedCountStream extends App {

  def parseEvents(eventStr: String): QueryResults = parse(eventStr).getOrElse(Json.Null).as[QueryResults] getOrElse null

  def parseRanked(eventStr: String): RankedClick = parse(eventStr).getOrElse(Json.Null).as[RankedClick] getOrElse null

  def getRankedResults(results: DataClasses.QueryResults): List[RankedClick] =
    results.results zip (1 to results.results.length) map { case (r, i) => RankedClick(i, r.clicked) } filter (_.clicked)

  val config: Properties = new Properties
  config.put(StreamsConfig.APPLICATION_ID_CONFIG, "pbm_clicks_test")
  config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
  config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
  config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
  val builder: StreamsBuilder = new StreamsBuilder()

  val queryResults: KStream[String, String] = builder.stream("pbm_results")
  val mappedLine = queryResults
    .mapValues(s => parseEvents(s))
    .filter((_, r) => r != null)
    .flatMapValues(queryResults => getRankedResults(queryResults))
    .selectKey((_, r) => r.rank)
    .groupBy((l, _) => l)
    .count(Named.as("count_by_rank"))
    .mapValues((n, c) => RankCount(n, c))
  mappedLine.toStream.to("ranked_output")

  val streams: KafkaStreams = new KafkaStreams(builder.build(), config)
  streams.start()

  sys.ShutdownHookThread {
    streams.close(Duration.ofSeconds(10))
  }
}
