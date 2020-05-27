package org.hpi.esb.datasender

import java.util.concurrent.TimeUnit
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.scalatest.FunSpec
import org.scalatest.mockito.MockitoSugar
import org.mockito.Mockito.{never, times, verify}
import org.mockito.{ArgumentMatchers, Mockito}
import scala.io.Source

class DataProducerThreadTest extends FunSpec with MockitoSugar {

  val topicA = "ESB_A_1"
  val topicB = "ESB_B_1"
  val topicC = "ESB_C_1"
  val topicD = "ESB_D_1"
  val ts: Long = 999
  val initialTopicId = 0
  var mockedDataProducer: DataProducer = mock[DataProducer]
  var mockedDataReader: DataReader = mock[DataReader]
  val duration = 10
  val durationTimeUnit = TimeUnit.MINUTES

  describe("send - multi column mode") {
    val topics = List(topicA, topicB, topicC)
    val singleColumnMode = false
    val mockedKafkaProducer: KafkaProducer[String, String] = mock[KafkaProducer[String, String]]
    val dataProducerThread = new DataProducerThread(mockedDataProducer,
      mockedKafkaProducer, mockedDataReader, topics, singleColumnMode, duration, durationTimeUnit)
    val spy = Mockito.spy(dataProducerThread)
    Mockito.doReturn(ts, Nil: _*).when(spy).currentTime


    it("should send each record value to the corresponding kafka topic") {
      val records = List("dat0", "dat1", "dat2")
      spy.send(Option(records))
      verify(mockedKafkaProducer, times(1)).send(new ProducerRecord[String, String](topics.head, s"$initialTopicId;$ts;" + records.head))
      verify(mockedKafkaProducer, times(1)).send(new ProducerRecord[String, String](topics(1), s"$initialTopicId;$ts;" + records(1)))
      verify(mockedKafkaProducer, times(1)).send(new ProducerRecord[String, String](topics(2), s"$initialTopicId;$ts;" + records(2)))
    }

    it("should not send anything to kafka when no record values are passed") {
      val mockedKafkaProducer = mock[KafkaProducer[String, String]]
      val dataProducerThread = new DataProducerThread(mockedDataProducer,
        mockedKafkaProducer, mockedDataReader, topics, singleColumnMode, duration, durationTimeUnit)
      dataProducerThread.send(None)
      verify(mockedKafkaProducer, never()).send(ArgumentMatchers.any(), ArgumentMatchers.any())
    }
  }

  describe("send - single column mode") {
    val topics = List(topicA, topicB, topicC)
    val singleColumnMode = true
    val mockedKafkaProducer: KafkaProducer[String, String] = mock[KafkaProducer[String, String]]
    val dataProducerThreadSC = new DataProducerThread(mockedDataProducer,
      mockedKafkaProducer, mockedDataReader, topics, singleColumnMode, duration, durationTimeUnit)
    val spySC = Mockito.spy(dataProducerThreadSC)
    Mockito.doReturn(ts, Nil: _*).when(spySC).currentTime

    it("should send the same record value to the corresponding kafka topic") {
      val records = List("dat0", "dat1", "dat2")
      spySC.send(Option(records))
      verify(mockedKafkaProducer, times(1)).send(new ProducerRecord[String, String](topics.head, s"$initialTopicId;$ts;" + records.head))
      verify(mockedKafkaProducer, times(1)).send(new ProducerRecord[String, String](topics(1), s"$initialTopicId;$ts;" + records.head))
      verify(mockedKafkaProducer, times(1)).send(new ProducerRecord[String, String](topics(2), s"$initialTopicId;$ts;" + records.head))
    }

    it("should increment message id correctly") {
      val records = List("dat0Topic0", "dat1Topic1", "dat2Topic0")
      val dataProducerThreadSC = new DataProducerThread(mockedDataProducer,
        mockedKafkaProducer, mockedDataReader, topics, singleColumnMode, duration, durationTimeUnit)
      val spySC = Mockito.spy(dataProducerThreadSC)
      Mockito.doReturn(ts, Nil: _*).when(spySC).currentTime
      spySC.send(Option(records))
      verify(mockedKafkaProducer, times(1)).send(new ProducerRecord[String, String](topics.head, s"$initialTopicId;$ts;" + records.head))
      verify(mockedKafkaProducer, times(1)).send(new ProducerRecord[String, String](topics(1), s"$initialTopicId;$ts;" + records.head))
      verify(mockedKafkaProducer, times(1)).send(new ProducerRecord[String, String](topics(2), s"$initialTopicId;$ts;" + records.head))
      spySC.send(Option(records))
      verify(mockedKafkaProducer, times(1)).send(new ProducerRecord[String, String](topics.head, s"${initialTopicId + 1};$ts;" + records.head))
      verify(mockedKafkaProducer, times(1)).send(new ProducerRecord[String, String](topics(1), s"${initialTopicId + 1};$ts;" + records.head))
      verify(mockedKafkaProducer, times(1)).send(new ProducerRecord[String, String](topics(2), s"${initialTopicId + 1};$ts;" + records.head))
    }

    it("should not send anything to kafka when no record values are passed") {
      val mockedKafkaProducer = mock[KafkaProducer[String, String]]
      val dataProducerThread = new DataProducerThread(mockedDataProducer,
        mockedKafkaProducer, mockedDataReader, topics, singleColumnMode, duration, durationTimeUnit)
      dataProducerThread.send(None)
      verify(mockedKafkaProducer, never()).send(ArgumentMatchers.any(), ArgumentMatchers.any())
    }
  }

  describe("run") {

    val topics = List(topicA, topicB, topicC)
    val singleColumnMode = false
    val columns = List("timestamp", "id", "stream0", "stream1", "stream2")
    val source: Source = Source.fromString(
      """ts id dat00 dat01 dat02
        |ts id dat10 dat11 dat12
      """.stripMargin)
    val duration = 1
    val durationTimeUnit = TimeUnit.MINUTES
    val dataReader = new DataReader(source, columns, columnDelimiter = " ", dataColumnStart = 2, readInRam = false)
    val mockedKafkaProducer: KafkaProducer[String, String] = mock[KafkaProducer[String, String]]

    it("should send records if the end time has not been reached") {
      val dataProducerThread = new DataProducerThread(mockedDataProducer,
        mockedKafkaProducer, dataReader, topics, singleColumnMode, duration, durationTimeUnit)
      val spy = Mockito.spy(dataProducerThread)
      Mockito.doReturn(ts, Nil: _*).when(spy).currentTime
      spy.run()
      verify(mockedKafkaProducer, times(1)).send(new ProducerRecord[String, String](topicA, s"$initialTopicId;$ts;dat00"))
      verify(mockedKafkaProducer, times(1)).send(new ProducerRecord[String, String](topicB, s"$initialTopicId;$ts;dat01"))
      verify(mockedKafkaProducer, times(1)).send(new ProducerRecord[String, String](topicC, s"$initialTopicId;$ts;dat02"))
    }

    it("should not send records if the time end has been reached") {
      val mockedKafkaProducer = mock[KafkaProducer[String, String]]
      val source: Source = Source.fromString("")
      val dataReader = new DataReader(source, columns, columnDelimiter = " ", dataColumnStart = 2, readInRam = false)
      val dataProducerThread = new DataProducerThread(mockedDataProducer,
        mockedKafkaProducer, dataReader, topics, singleColumnMode, duration, durationTimeUnit)
      val spy = Mockito.spy(dataProducerThread)
      Mockito.doReturn(System.currentTimeMillis()*1000 , Nil: _*).when(spy).currentTime
      spy.run()
      verify(mockedKafkaProducer, never()).send(ArgumentMatchers.any[ProducerRecord[String, String]])
    }
  }
}
