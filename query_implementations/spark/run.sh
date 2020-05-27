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
JAR_FILE=`find target -name "*assembly*.jar" -type f`
OUTPUT_DIR=$HOME/testoutput_dir/
CHECKPOINT_DIR="hdfs://node-master:19000/spark"

for (( c=1; c<=$NUMBER_OF_RUNS; c++ ))
do
    stopSpark
    #echo "sleeping for 10s"
    sleep 15
    #unfortunately, jobs wont get killed automatically
    kill -9 `pgrep -f spark`
    sleep 5
    hadoop fs -rm -R /spark/beam-checkpoint
    sleep 5
    hadoop fs -rm -R /spark/spark-checkpoint
    sleep 5
    hadoop fs -rm -R "/spark/*"
    sleep 5
    startSpark
    sleep 15
    $SPARK_HOME/bin/spark-submit --master spark://xxx.xxx.xx.xxx:7077 $JAR_FILE $QUERY --parallelism=$PARALLELISM --run=$c --checkpointDir=$CHECKPOINT_DIR >output_$c.log 2>&1 &
    sleep 120
done
