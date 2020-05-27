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
export FLINK_HOME=/opt/flink

for (( c=1; c<=$NUMBER_OF_RUNS; c++ ))
do
    stopFlink
    echo "sleeping for 10s"
    sleep 10
    startFlink
    sleep 10
    $FLINK_HOME/bin/flink run -p $PARALLELISM -c de.hpi.epic.beam.App ./target/spbenchmark-beam-bundled-1.0.jar --runner=FlinkRunner --benchmark=$QUERY --run=$c --output=$HOME/testoutput_dir/ > output_$c.log 2>&1 &
    sleep 120
done
