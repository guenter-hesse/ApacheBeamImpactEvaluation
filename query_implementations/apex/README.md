# Apex Apex Implementation

Query implementation for Apache Apex.

## Preparation
- Define the Apache Kafka bootstrap servers, input topic, and output topic (each w/o suffix) in ```src/main/scala/sb_metric_calc/MetricCalculator```
- Define the Apache Kafka topic name (w/o number suffix) in ```src/main/scala/sb_metric_calc/MetricCalculator```
- Adapt ```run.config``` if needed
- Adapt paths in ```run.sh```

## Execution

Execute:

1. ```mvn clean package -Dmaven.test.skip=true```
2. ```./run.sh```
