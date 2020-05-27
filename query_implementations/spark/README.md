# Apex Spark Streaming Implementation

Query implementation for Apache Apex.

## Preparation
- Define the Apache Kafka bootstrap servers, input topic, and output topic (each w/o suffix) in ```src/main/scala/sb_spark/StartBenchmark```
- Define the checkpoint directory in ```src/main/scala/sb_spark/util/Config```
- Adapt ```run.config``` if needed
- Adapt paths in ```run.sh```

## Execution

Execute:

1. ```sbt clean assembly```
2. ```./run.sh```
