scalaVersion := "2.12.18"

libraryDependencies += "com.bedrockstreaming" %% "sparktest" % "0.3.0" % "test"

val ScalacticVersion = "3.2.11"

// ************
// Dependencies
// ************
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.2.1"
libraryDependencies += "io.github.embeddedkafka" %% "embedded-kafka" % "3.6.1" % Test
libraryDependencies += "org.scalactic" %% "scalactic" % ScalacticVersion
libraryDependencies += "org.scalatest" %% "scalatest" % ScalacticVersion % "test"
libraryDependencies += "com.lihaoyi" %% "upickle" % "3.1.4"
libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.6.12"