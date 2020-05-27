package benchmark

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{State, StateSpec}
import util.Config

class StatisticsBenchmark(config: Config) extends Benchmark  {
  private val projectedFieldIdx = 2
  private val delimiter = "\t"

  def statisticsMapping(key: Unit, value: Option[Double], state: State[(Double, Double, Double, Long)] ) : String = {
    if (! state.exists()) state.update((Double.NaN, Double.NaN, 0d, 0L))
    var (maxField : Double, minField : Double, sum : Double, count : Long) = state.get
    val field = value.get

    count += 1 
    if (maxField.isNaN) {
      maxField = field
    }
    else
    {
      maxField = math.max(field, maxField)
    }

    if (minField.isNaN) {
      minField = field
    }
    else
    {
      minField = math.min(field, minField)
    }
    sum += field
    state.update((maxField, minField, sum, count))
    "Max:\t" + maxField + ", Min:\t" + minField + ", Sum:\t" + sum + ", Avg:\t" + sum/count
  }

  def splitted(line: String) : (Unit, Double) = {
    return ({}, line.split(delimiter)(projectedFieldIdx).toDouble)
  }

  def processing(stream: DStream[String]) : DStream[String] =  {
    // The usage of key 1 causes the behaviour of a "global" state
    stream.map(x => splitted(x))
          .mapWithState(StateSpec.function(statisticsMapping _))
  }

  def execute(input: DStream[String])
  =
    this.processing(input)
      //.saveAsTextFiles(config.outDir.getCanonicalPath + "/res")

}
