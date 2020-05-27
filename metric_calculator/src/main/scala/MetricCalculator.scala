package sb_metric_calc

object MetricCalculator extends App {

  val queryList: List[String] = List("Grep") //List("Grep", "Identity", "Sample", "Projection")
  val brokers: String = "xxx.xxx.xx.xxx:xxxx,xxx.xxx.xx.xxx:xxxx,xxx.xxx.xx.xxx:xxxx"
  val groupId: String = java.util.UUID.randomUUID.toString

  var numberOfRuns: Int = 1
  if (!args.isEmpty)
    numberOfRuns = args(0).toInt

  calcLatencies()

  def getTopicList(numberOfRuns: Int): Map[String, List[String]] = {
    var topicList: Map[String, List[String]] = Map[String, List[String]]()
    queryList.foreach { query: String =>
      var topicListForQuery: List[String] = List[String]()
      for (i <- 1 to numberOfRuns) {
        topicListForQuery = s"TOPIC_NAME_WITH_INDEX_SUFFIX_$i" :: topicListForQuery
      }
      topicList += (query -> topicListForQuery)
    }
    topicList
  }

  def calcLatencies(): Unit =  calcLatencyForTopicList(getTopicList(numberOfRuns))

  def calcLatencyForTopicList(queryTopicMap: Map[String, List[String]]): Map[String, Double] = {

    var resultMap: Map[String, Double] = Map[String, Double]()
    var sum: Double = 0
    queryTopicMap foreach {
      case (query, topicList) => {
        var numberOfTopic: Int = 1
        topicList.foreach { topic: String =>
          val nRecAndFirstAndLastTS: Map[String, Long] = new SBKafkaConsumer(brokers, java.util.UUID.randomUUID.toString, topic).getNumberOfRecordsAndFirstAndLastTS
          println(nRecAndFirstAndLastTS)
          val lastTS = nRecAndFirstAndLastTS("lastTS")
          val firstTS = nRecAndFirstAndLastTS("firstTS")
          sum += (lastTS - firstTS)
          val resList: List[String] = List[String](query, topic, firstTS.toString, lastTS.toString, (lastTS-firstTS).toString, sum.toString, (sum/numberOfTopic).toString)
          ResultWriter.write(resList)
          numberOfTopic += 1
        }
      }
    }
    resultMap += ("avgLatency" -> (sum/numberOfRuns))
    resultMap
  }

}
