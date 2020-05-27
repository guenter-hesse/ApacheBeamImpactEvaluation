#!/usr/bin/env bash

JAR_FILE="streambenchapex-1.0-SNAPSHOT.apa"
APEX_HOME="/opt/apache-apex"
REPO_HOME="/xxx/xxx/xxx/SPBenchmark/query_implementations/apex"

. ./run.config

$HADOOP_HOME/sbin/stop-yarn.sh
sleep 10
$HADOOP_HOME/sbin/stop-dfs.sh
sleep 10
$HADOOP_HOME/sbin/start-dfs.sh
sleep 10
$HADOOP_HOME/sbin/start-yarn.sh
sleep 10
./killAllYarnApps.sh 
sleep 10

for (( c=1; c<=$NUMBER_OF_RUNS; c++ ))
do
    echo "executing: $APEX_HOME/engine/src/main/scripts/apex -vvv -e \"launch $REPO_HOME/target/streambenchapex-1.0-SNAPSHOT.apa -D benchmark=$QUERY -Drun=$c -Dparallelism=$PARALLELISM\"  > output$c.log 2>&1"
    nohup $APEX_HOME/engine/src/main/scripts/apex -vvvv -e "launch -D benchmark=$QUERY -D run=$c -D parallelism=$PARALLELISM $REPO_HOME/target/streambenchapex-1.0-SNAPSHOT.apa" > output$c.log 2>&1 &
    sleep 600
    ./killAllYarnApps.sh
    sleep 20
    $HADOOP_HOME/sbin/stop-yarn.sh
    sleep 10
    $HADOOP_HOME/sbin/stop-dfs.sh
    sleep 10
    $HADOOP_HOME/sbin/start-dfs.sh
    sleep 10
    $HADOOP_HOME/sbin/start-yarn.sh
    sleep 10
done
