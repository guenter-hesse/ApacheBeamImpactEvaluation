package benchmark

import org.apache.spark.streaming.dstream.DStream
import util.Config

case class SamplingBenchmark(config: Config) extends AbstractBenchmark (config) {
  private val random = new java.util.Random()
  private val sampleProbability = this.config.probability % 100 // = 40.0 / 100
  // Test probability

  def processing(stream: DStream[String]): DStream[String] = {
      stream.filter(line => {
        random.nextInt(100) <= sampleProbability
      })
  }
}
