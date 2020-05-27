package benchmark

import org.apache.spark.streaming.dstream.DStream

trait Benchmark extends java.io.Serializable
{
  def execute(input: DStream[String]) : DStream[String]
}
