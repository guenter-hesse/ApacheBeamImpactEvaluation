#!/usr/bin/env bash

function sendDataToTopics {
    echo "Creating topics:"
    START=1
    END=$1
    for (( c=$START; c<=$END; c++ ))
    do
        head -1 tools/commons/commons.conf
        java -Xms12g -jar tools/datasender/target/scala-2.11/DataSender-assembly-0.1.0-SNAPSHOT.jar
        sleep 2
        NEW_C=$((c+1))
        sed -i '' "1s/\(.*\)$c/\1$NEW_C/" tools/commons/commons.conf
    done
}

. ./run.config
sendDataToTopics $NUMBER_OF_RUNS
