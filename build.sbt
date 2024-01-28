scalaVersion := "2.12.18"

val ScalacticVersion = "3.2.11"

// ************
// Dependencies
// ************
libraryDependencies ++= Seq(
  "org.apache.spark"        %% "spark-sql-kafka-0-10" % "3.2.1",
  "org.scalactic"           %% "scalactic"            % ScalacticVersion,
  "com.lihaoyi"             %% "upickle"              % "3.1.4",
  "org.json4s"              %% "json4s-jackson"       % "3.6.12",
  "com.bedrockstreaming"    %% "sparktest"            % "0.3.0"          % Test,
  "io.github.embeddedkafka" %% "embedded-kafka"       % "3.6.1"          % Test,
  "org.scalatest"           %% "scalatest"            % ScalacticVersion % Test)