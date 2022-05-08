package com.dreamteam.clickmodels
package Spark.Compaction

import Spark.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.row_number

import scala.util.{Failure, Success, Try}

object Compaction {

  import spark.implicits._

  case class CompactedDf(df: DataFrame) {
    object serialize {
      def parquet(path: String): Unit = df.write.parquet(path)

      def json(path: String): Unit = df.write.json(path)
    }
  }

  case object CompactedDf {
    def json(pathForCompaction: String, groupCount: Int): Either[CompactedDf, String] = {
      tryCompact(pathForCompaction, groupCount, spark.read.json)
    }

    def parquet(pathForCompaction: String, groupCount: Int): Either[CompactedDf, String] = {
      tryCompact(pathForCompaction, groupCount, spark.read.parquet)
    }

    def dataFrame(df: DataFrame, groupCount: Int): Either[CompactedDf, String] = {
      val window: WindowSpec = Window.partitionBy("query").orderBy($"timestamp".desc)
      val compacted: DataFrame = df.withColumn("rn", row_number().over(window)).filter($"rn" <= groupCount)
      Left(CompactedDf(compacted))
    }

    private def tryCompact(path: String, groupCount: Int, approach: String => DataFrame): Either[CompactedDf, String] = {
      val eDf: Either[DataFrame, String] = tryRead(path, approach)
      eDf.left.flatMap(d => dataFrame(d, groupCount))
    }

    private def tryRead(path: String, frameReader: String => DataFrame): Either[DataFrame, String] = Try(
      frameReader(path)
    ) match {
      case Success(value) => Left(value)
      case Failure(exception) => Right(s"Can't read dataframe for compaction. ${exception.getCause}")
    }
  }
}
