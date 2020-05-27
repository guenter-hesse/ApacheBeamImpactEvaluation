# Metric Calculator

The metric calculator for computing execution times for a series of topics/runs. Results will be written to csv files.

## Preparation
- Define the Apache Kafka bootstrap servers in ```src/main/scala/sb_metric_calc/MetricCalculator```
- Define the Apache Kafka topic name (w/o number suffix) in ```src/main/scala/sb_metric_calc/MetricCalculator```

## Execution

Execute
```sbt "run x"```
, whereby ```x``` denotes the number of runs/topics.