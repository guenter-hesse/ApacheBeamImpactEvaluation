package sb_metric_calc

import scala.collection.JavaConversions._
import java.util.concurrent._
import java.util.{Collections, Properties}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import com.typesafe.scalalogging._

class SBKafkaConsumer(val brokers: String,
                      val groupId: String,
                      val topic: String) extends StrictLogging {

  val props: Properties = createConsumerConfig(brokers, groupId)
  val consumer = new KafkaConsumer[String, String](props)
  var executor: ExecutorService = _

  def shutdown(): Unit = {
    if (consumer != null)
      consumer.close()
    if (executor != null)
      executor.shutdown()
  }

  def createConsumerConfig(brokers: String, groupId: String): Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10000000")
    props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "10000000")
    props
  }

  def getNumberOfRecordsAndFirstAndLastTS: Map[String, Long] = {
    consumer.subscribe(Collections.singletonList(topic))
    consumer.poll(10) //dummy poll needed for seek
    consumer.seek(new TopicPartition(topic, 0), 0)
    var recordCountOutput = 0
    var firstElementTS: Long = -1
    var lastElementTS: Long = -1

    var records = consumer.poll(1000)
    while (records.count() > 0) {
      recordCountOutput += records.count
      for (record <- records.iterator()) {
        if (firstElementTS < 0)
          firstElementTS = record.timestamp()
        lastElementTS = record.timestamp() //TODO: possible to get last record w/o iterating over alle records? perhaps using offset?
      }
      //println(s"${records.count()} number of records for topic: $topic")
      records = consumer.poll(1000)
    }

    Map(
      "numberOfRecords" -> recordCountOutput.toLong,
      "firstTS" -> firstElementTS,
      "lastTS" -> lastElementTS
    )
  }
}