lazy val root = (project in file(".")).
  settings(
    name := "spbenchmark_spark_native",
    version := "1.0",
    // ATTENTION: the scalaVersion must be supported by the spark version (e.g. spark 1.6.2 supports scala 2.10.6)
    scalaVersion := "2.11.8",
    mainClass in Compile := Some("StartBenchmark")
)

libraryDependencies ++= Seq(
  "com.github.scopt" %% "scopt" % "3.5.0",
  "org.apache.spark" %% "spark-core" % "2.3.0" % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.3.0",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.3.0",
  "com.google.guava" % "guava" % "19.0"
)

// META-INF discarding
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}