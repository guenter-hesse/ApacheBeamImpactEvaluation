package benchmark

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{State, StateSpec, StreamingContext}
import util.Config

class DistinctCountBenchmark(config: Config) extends Benchmark 
{
  private val projectedFieldIdx = 2
  private val delimiter = "\t"

  def distinctMapping(key: Unit, value: Option[String], state: State[Map[String, Unit]] ) : Long = {
    var l = Map[String, Unit]()
    if (state.exists()) l = state.get

    l += value.get -> {}
    state.update(l)
    l.size.longValue
  }

  def processing(stream: DStream[String]) : DStream[Long] =  {
    stream.map( x => ({}, (x.split(delimiter)(projectedFieldIdx))) )
          .mapWithState(StateSpec.function(distinctMapping _))
  }

  def execute(input: DStream[String]): DStream[String] = this.processing(input).map(_.toString)
}
