package benchmark

import org.apache.spark.streaming.dstream.DStream
import util.Config

class ProjectionBenchmark(config: Config) extends AbstractBenchmark (config) {
  private val projectedFieldIdx = 2
  private val delimiter = "\t"

  def processing(stream: DStream[String]): DStream[String] = {
    stream.map(l => {
      val fields = l.split(delimiter)
      fields(projectedFieldIdx)
    })
  }
}
