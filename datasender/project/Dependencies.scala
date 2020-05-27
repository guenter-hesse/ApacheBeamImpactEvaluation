import sbt._

object Dependencies {

  val testDependencies = Seq(
    "org.scalamock" % "scalamock-scalatest-support_2.11" % "3.6.0" % Test,
    "org.mockito" % "mockito-core" % "2.8.9" % Test,
    "org.scalactic" % "scalactic_2.11" % "3.0.4" % Test,
    "org.scalatest" % "scalatest_2.11" % "3.0.4" % Test
  )

  val kafkaClients = Seq(
    "org.apache.kafka" % "kafka-clients" % "0.10.1.0"
  )

  val kafka = Seq(
    "org.apache.kafka" % "kafka_2.11" % "0.10.1.0"
  )

  val loggingDependencies = Seq(
    "log4j" % "log4j" % "1.2.14"
  )

  val scalaIODependencies = Seq(
    "com.github.scala-incubator.io" % "scala-io-core_2.11" % "0.4.3-1",
    "com.github.scala-incubator.io" % "scala-io-file_2.11" % "0.4.3-1"
  )

  val configHandlingDependency = Seq(
    "com.github.scopt" %% "scopt" % "3.5.0",
    "com.github.melrief" %% "pureconfig" % "0.5.0"
  )

  val csv = Seq(
    "com.github.tototoshi" %% "scala-csv" % "1.3.4"
  )

  val json = Seq(
    "net.liftweb" %% "lift-json" % "3.0.1"
  )

}
