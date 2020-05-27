#!/usr/bin/env bash

function stopFlink {
    echo :Stopping Flink
    echo "executing \$FLINK_HOME/bin/stop-cluster.sh"
    $FLINK_HOME/bin/stop-cluster.sh
}

function startFlink {
    echo :Starting Flink
    echo "executing \$FLINK_HOME/bin/start-cluster.sh"
    $FLINK_HOME/bin/start-cluster.sh
}

. ./run.config
export FLINK_HOME="/opt/flink"

for (( c=1; c<=$NUMBER_OF_RUNS; c++ ))
do
    stopFlink
    echo "sleeping for 10s"
    sleep 15
    startFlink
    sleep 15
    nohup /opt/flink/bin/flink run -p $PARALLELISM "target/FlinkBenchmark-0.1.0.jar" "$QUERY" "localhost" "9999" "logs" "$c" > log_$QUERY_$c.out 2>&1 &
    sleep 90
done
