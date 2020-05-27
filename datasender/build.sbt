import Dependencies._

name := "DatasenderSB"
parallelExecution in Test := false

lazy val commonSettings = Seq(
  organization := "org.hpi",
  version := "0.1.0-SNAPSHOT",
  scalaVersion := "2.11.8",
  test in assembly := {},
  logBuffered in test := false,
  parallelExecution in Test := false
)

lazy val root = (project in file(".")).
  settings(commonSettings).
  aggregate(datasender, util)

lazy val commons = (project in file("tools/commons")).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= configHandlingDependency,
    libraryDependencies ++= scalaIODependencies,
    libraryDependencies ++= loggingDependencies,
    libraryDependencies ++= csv,
    libraryDependencies ++= testDependencies
  ).
  settings(
    name := "Commons"
  )

lazy val datasender = (project in file("tools/datasender")).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= kafkaClients,
    libraryDependencies ++= loggingDependencies,
    libraryDependencies ++= scalaIODependencies,
    libraryDependencies ++= configHandlingDependency,
    libraryDependencies ++= testDependencies
  ).
  settings(
    name := "DataSender",
    mainClass in assembly := Some("org.hpi.esb.datasender.Main")
  ).
  dependsOn(commons, util)

lazy val util = (project in file("tools/util")).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= kafka,
    libraryDependencies ++= configHandlingDependency,
    libraryDependencies ++= testDependencies,
    libraryDependencies ++= json
  ).
  settings(
    name := "Util",
    mainClass in (Compile,run) := Some("org.hpi.esb.util.Main")
  ).
  dependsOn(commons)
