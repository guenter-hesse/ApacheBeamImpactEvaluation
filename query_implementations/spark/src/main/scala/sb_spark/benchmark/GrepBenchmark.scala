package benchmark

import util.Config
import org.apache.spark.streaming.dstream.DStream

class GrepBenchmark(config: Config) extends AbstractBenchmark (config) {

  def processing(stream: DStream[String]): DStream[String] = {
    stream.filter(_.contains(this.config.grepFilter))
  }
}
