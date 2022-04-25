ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.2.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.0" % "provided"
libraryDependencies += "org.postgresql" % "postgresql" % "42.3.4"
libraryDependencies += "com.github.pureconfig" %% "pureconfig" % "0.17.1"
libraryDependencies += "org.apache.kafka" %% "kafka-streams-scala" % "3.1.0"
libraryDependencies += "joda-time" % "joda-time" % "2.10.14"

val circeVersion = "0.14.1"

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)


lazy val root = (project in file("."))
  .settings(
    name := "DistributedPBMModel",
    idePackagePrefix := Some("com.dreamteam.clickmodels")
  )
