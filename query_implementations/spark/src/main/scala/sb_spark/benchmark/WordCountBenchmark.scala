package benchmark

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{State, StateSpec, StreamingContext}
import util.Config

class WordCountBenchmark(config: Config) extends Benchmark
{
  def wordCountMapping(key: String, value: Option[Int], state: State[Long] ) : String = {
      val sum = value.getOrElse(0).toLong + state.getOption.getOrElse(0L)
      val output = key + ": " + sum.toString
      state.update(sum)
      output
  }

  def processing(stream: DStream[String]) : DStream[String] =  {
    stream.flatMap( _.split("\\s+").map ((_,1)) )
          .mapWithState(StateSpec.function(wordCountMapping _))
  }

  def execute(input: DStream[String])
  =
    this.processing(input)
     // .saveAsTextFiles(config.outDir.getCanonicalPath + "/res")

}
