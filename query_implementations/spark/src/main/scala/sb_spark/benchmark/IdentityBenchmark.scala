package benchmark

import org.apache.spark.streaming.dstream.DStream
import util.Config

case class IdentityBenchmark(config: Config) extends AbstractBenchmark (config) {
  def processing(stream: DStream[String]) : DStream[String] = {
    stream
  }
}
