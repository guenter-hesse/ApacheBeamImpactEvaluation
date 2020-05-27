#!/usr/bin/env bash

function stopDfs {
    echo :Stopping DFS
    echo "executing \$HADOOP_HOME/sbin/stop-dfs.sh"
    $HADOOP_HOME/sbin/stop-dfs.sh
}

function startDfs {
    echo :Starting DFS
    echo "executing \$HADOOP_HOME/sbin/start-dfs.sh"
    $HADOOP_HOME/sbin/start-dfs.sh
}

function stopYarn {
    echo :Stopping YARN
    echo "executing \$HADOOP_HOME/sbin/stop-yarn.sh"
    $HADOOP_HOME/sbin/stop-yarn.sh
}

function startYarn {
    echo :Starting YARN
    echo "executing \$HADOOP_HOME/sbin/start-yarn.sh"
    $HADOOP_HOME/sbin/start-yarn.sh
}

. ./run.config
export HADOOP_HOME=/opt/hadoop
export APEX_HOME=/opt/apache-apex

stopYarn
echo "sleeping for 10s"
sleep 10
stopDfs
echo "sleeping for 10s"
sleep 10
startDfs
echo "sleeping for 10s"
sleep 10
startYarn
echo "sleeping for 10s"
sleep 10

for (( c=1; c<=$NUMBER_OF_RUNS; c++ ))
do
    echo "executing \"java -cp \"target/spbenchmark-beam-bundled-1.0.jar:src/main/resources/*\" de.hpi.epic.beam.App --benchmark=$QUERY --run=$c --parallelism=$PARALLELISM --runner=ApexRunner --output=$HOME/testoutput_dir/ --embeddedExecution=false --applicationName=\"apexbeamapp\" --configFile=\"src/main/resources/beam-runners-apex.properties\""
    java -cp "target/spbenchmark-beam-bundled-1.0.jar:src/main/resources/*" de.hpi.epic.beam.App --benchmark=$QUERY --run=$c --parallelism=$PARALLELISM --runner=ApexRunner --output=$HOME/testoutput_dir/ --embeddedExecution=false --applicationName="apexbeamapp" --configFile="src/main/resources/beam-runners-apex.properties"
    sleep 600
    echo "killing all YARN apps"
    ./killAllYarnApps.sh
    echo "sleeping for 10s"
    sleep 10
    stopYarn
    echo "sleeping for 10s"
    sleep 10
    stopDfs
    echo "sleeping for 10s"
    sleep 10
    startDfs
    echo "sleeping for 10s"
    sleep 10
    startYarn
    echo "sleeping for 10s"
    sleep 10
done
