# Apex Beam Implementations

Query implementation with Apache Beam.

## Preparation
- Define the Apache Kafka bootstrap servers, input topic, and output topic (each w/o suffix) in ```src/main/java/de/hpi/epic/beam/App.java```
- Adapt ```run.config``` if needed
- Adapt paths in ```run*.sh``` files

## Execution

Following steps are needed:

1. Build the project
   1. ```mvn clean package -P apex-runner``` for Apache Apex
   1. ```mvn clean package -P spark-runner``` for Apache Spark Streaming
   1. ```mvn clean package``` for Apache Flink
1. ```./runSBBeamApex.sh``` or ```./runSBBeamFlink.sh``` or ```./runSBBeamSpark.sh```, depending on where you want to execute the query