package com.dreamteam.clickmodels
package Kafka.Streams
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.scala.kstream.{KStream, KTable}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.kstream.Named

import java.time.Duration
import java.util.Properties
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

object KafkaTestStream extends App {
  val form: DateTimeFormatter = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss")
  case class NameCount(name: String, count: Long, timestamp: String = form.print(DateTime.now()))

  implicit val serdeNC: Serde[NameCount] = {
    val ser: NameCount => Array[Byte] = (n: NameCount) => n.asJson.noSpaces.getBytes
    val deser: Array[Byte] => Option[NameCount] = (a: Array[Byte]) => {
      val str = new String(a)
      decode[NameCount](str).toOption
    }
    fromFn(ser, deser)
  }
  val config: Properties = new Properties
  config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-scala")
  config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
  config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
  config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
  val builder: StreamsBuilder = new StreamsBuilder()

  // Step 1: We create the topic of users keys to colours
  val textLines: KStream[String, String] = builder.stream("testInput")
  val mappedLine = textLines
    .selectKey((_, v) => v.split(",")(0).toLong)
    .mapValues(v => v.split(",")(1).toLowerCase)
  mappedLine.to("testInter")

  val usersAndColoursTable: KTable[Long, String] = builder.table("testInter")

  val count = usersAndColoursTable
    .groupBy((_, v) => (v, v))
    .count(Named.as("count_by_name"))
    .mapValues((n, c) => NameCount(n, c))
  count.toStream.to("testOutput")

  val streams: KafkaStreams = new KafkaStreams(builder.build(), config)
  streams.start()

  sys.ShutdownHookThread {
    streams.close(Duration.ofSeconds(10))
  }
}
