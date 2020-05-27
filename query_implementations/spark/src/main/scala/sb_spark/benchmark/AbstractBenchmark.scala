package benchmark

import org.apache.spark.streaming.dstream.DStream
import util.Config

abstract class AbstractBenchmark(config: Config) extends Benchmark {
  def processing(stream: DStream[String]): DStream[String]
  def execute(input: DStream[String]) : DStream[String] = this.processing(input)
      //.saveAsTextFiles(config.outDir.getCanonicalPath + "/res")
}
