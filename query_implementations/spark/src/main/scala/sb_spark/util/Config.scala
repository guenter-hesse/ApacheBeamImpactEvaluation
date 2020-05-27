package util

import java.io.File
import benchmark._

case class Config(
  checkpointDir: String = "hdfs://node-master:9000/spark",
  benchmark: String = "",
  parallelism: Int = 1,
  run: Int = 1,
  probability: Double = 40.0,
  grepFilter: String = "test"
)
