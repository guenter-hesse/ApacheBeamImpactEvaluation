lazy val root = (project in file(".")).
  settings(
    name := "MetricCalculator",
    version := "1.0",
    scalaVersion := "2.12.4",
    mainClass in Compile := Some("sb_metric_calc.MetricCalculator")
  )

// https://mvnrepository.com/artifact/org.apache.kafka/kafka
libraryDependencies += "org.apache.kafka" %% "kafka" % "1.0.0"
// https://mvnrepository.com/artifact/com.typesafe.scala-logging/scala-logging
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2"

//cancelable in Global := true