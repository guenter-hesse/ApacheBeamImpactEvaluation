# Data Sender

This is the tool developed for sending input data to Apache Kafka topics.

## Topic Creation

For topic creation, the script ```createKafkaTopics.sh``` could be used (IPs, paths, topic names might need to be adapted beforehand).

## Preparation
- Define the number of topics you would like to send data to in ```run.config```
- Define the topic name ending with suffix ```1``` in ```tools/commons.conf - topicPrefix```
- Define the path to the input data file as well as the Apache Kafka bootstrap servers in ```tools/datasender/datasender.conf - kafkaProducerConfig.bootstrapServers, dataReaderConfig.dataInputPath```
- Define the Apache Kafka bootstrap servers in ```util/src/main/scala/org/hpi/esb/util/OffsetManagement```
- Define the Apache Kafka brokers and Zookeeper in ```util/src/main/scala/org/hpi/esb/util/TopicManagement```

## Execution
For execution, basically two steps are needed:

1. Build the project by executing:
```sbt clean assembly```
2. Run the script for sending data to the Apache Kafka topic(s):
```./sendDataToTopics.sh```
