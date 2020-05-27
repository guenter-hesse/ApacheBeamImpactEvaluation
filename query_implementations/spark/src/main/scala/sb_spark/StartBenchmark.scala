import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import util._
import benchmark._
import java.io.File
import java.util.Properties
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import sb_spark.util.KafkaSink

object StartBenchmark {
  val (grep, identity, sampling, projection, wordcount, statistic, distinct) = ("Grep", "Identity", "Sample", "Projection", "WordCount", "Statistics", "DistinctCount")

  val bootstrapServers = "xxx.xxx.xx.xxx:xxxx,xxx.xxx.xx.xxx:xxxx,xxx.xxx.xx.xxx:xxxx"

  def getBenchmarkByType(config: Config) : Benchmark = config.benchmark match {
    case `grep`       => new GrepBenchmark(config)
    case `identity`   => new IdentityBenchmark(config)
    case `sampling`   => new SamplingBenchmark(config)
    case `projection` => new ProjectionBenchmark(config)
    case `statistic`  => new StatisticsBenchmark(config)
    case `wordcount`  => new WordCountBenchmark(config)
    case `distinct`   => new DistinctCountBenchmark(config)
  }

  def main (args: Array[String])
  {
    val config = handleArguments(args)
    executeBenchmark(config)
  }

  def executeBenchmark(config: Config)
  {

    val kafkaParamsAsMap = Map[String, Object](
      "bootstrap.servers" -> bootstrapServers,
      "group.id" -> "spsparknative",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "earliest"
    )

    val kafkaParamsAsProps = new Properties()
    kafkaParamsAsProps.put("bootstrap.servers", bootstrapServers)
    //kafkaSinkConf.put("client.id", "ScalaProducerExample")
    kafkaParamsAsProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaParamsAsProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val appName = this.getClass.getSimpleName

    val sparkConf = new SparkConf().setAppName(config.benchmark + "Benchmark")
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(1))
    ssc.checkpoint(config.checkpointDir)

    val kafkaSink = sc.broadcast(KafkaSink(kafkaParamsAsProps))

    val inputTopic = s"INPUT_TOPIC_NAME_WITH_INDEX_SUFFIX_${config.run}"
    val outputTopic = s"TOPIC_NAME_WITH_INDEX_SUFFIX_${config.run}"

    val input = KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[Void, String](Array(inputTopic), kafkaParamsAsMap)).map(_.value())
    getBenchmarkByType(config).execute(input).foreachRDD { rdd =>
      rdd.foreach { message: String =>
        kafkaSink.value.send(outputTopic ,message)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

  def handleArguments (args: Array[String]): Config =
  {
    val parser = new scopt.OptionParser[Config]("StartBenchmark") {
      help("help").text("prints this usage text")

      opt[String]("checkpointDir").action( (x, c) => c.copy(checkpointDir = x) )
      opt[Int]("parallelism").action( (x, c) => c.copy(parallelism = x) )
      opt[Int]("run").action( (x, c) => c.copy(run = x) )
      cmd(identity).action( (_, c) => c.copy(benchmark = identity) ).text("start a identity benchmark")
      cmd(wordcount).action( (_, c) => c.copy(benchmark = wordcount) ).text("start a wordcount benchmark")
      cmd(distinct).action( (_, c) => c.copy(benchmark = distinct) ).text("start a distinct benchmark")
      cmd(statistic).action( (_, c) => c.copy(benchmark = statistic) ).text("start a statistics benchmark")
      cmd(projection).action( (_, c) => c.copy(benchmark = projection) ).text("start a projection benchmark")
      cmd(sampling).action( (_, c) => c.copy(benchmark = sampling) ).text("start a sampling benchmark")
      cmd(grep).action( (_, c) => c.copy(benchmark = grep) ).text("start a grep benchmark")

      checkConfig ( c => 
         if (c.benchmark.isEmpty) failure("Benchmark command is missing !")
         else {
           println(s"Config:\n-benchmark:${c.benchmark}\n-parallelism:${c.parallelism}\n-run:${c.run}")
           success
         }
      )
    }

    parser.parse(args, Config()) match {
      case Some(config) =>
	      config
      case None =>
        println("Error parsing arguments")
        sys.exit(1)
    }
  }
}
