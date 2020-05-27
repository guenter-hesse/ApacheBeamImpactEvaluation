#!/usr/bin/env bash

function stopSpark {
    echo :Stopping spark
    echo "executing \$SPARK_HOME/sbin/stop-all.sh"
    $SPARK_HOME/sbin/stop-all.sh
}

function startSpark {
    echo :Starting spark
    echo "executing \$SPARK_HOME/sbin/start-all.sh"
    $SPARK_HOME/sbin/start-all.sh
}

. ./run.config
export SPARK_HOME="/opt/spark"

for (( c=1; c<=$NUMBER_OF_RUNS; c++ ))
do
    stopSpark
    sleep 15
    kill -9 `pgrep -f spark`
    sleep 5
    hadoop fs -rm -r hdfs://node-master:19000/spark
    sleep 10
    startSpark
    sleep 15
    echo "hadoop fs -ls hdfs://node-master:19000/spark/"
    hadoop fs -ls hdfs://node-master:19000/spark/
    $SPARK_HOME/bin/spark-submit --master spark://xxx.xxx.xx.x:7077 --class=de.hpi.epic.beam.App target/spbenchmark-beam-bundled-1.0.jar --runner=SparkRunner --checkpointDir="hdfs://node-master:19000/spark"
    --benchmark=$QUERY --run=$c --output=$HOME/testoutput_dir/ >output_$c.log 2>&1 &
    sleep 120
done
