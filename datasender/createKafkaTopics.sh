#!/usr/bin/env bash

function createTopics {
    echo "Creating topics:"
    START=1
    END=$1
    for (( c=$START; c<=$END; c++ ))
    do
    echo "/opt/kafka/bin/kafka-topics.sh --create --zookeeper xxx.xxx.xx.xxx:xxxx,xxx.xxx.xx.xxx:xxxx,xxx.xxx.xx.xxx:xxxx --replication-factor 1 --partitions 1 --topic INPUT_TOPIC_NAME_WITH_INDEX_SUFFIX_$c"
        /opt/kafka/bin/kafka-topics.sh --create --zookeeper xxx.xxx.xx.xxx:xxxx,xxx.xxx.xx.xxx:xxxx,xxx.xxx.xx.xxx:xxxx --replication-factor 1 --partitions 1 --topic INPUT_TOPIC_NAME_WITH_INDEX_SUFFIX_$c
        sleep 2
        echo "/opt/kafka/bin/kafka-topics.sh --create --zookeeper xxx.xxx.xx.xxx:xxxx,xxx.xxx.xx.xxx:xxxx,xxx.xxx.xx.xxx:xxxx --replication-factor 1 --partitions 1 --topic TOPIC_NAME_WITH_INDEX_SUFFIX_$c"
        /opt/kafka/bin/kafka-topics.sh --create --zookeeper xxx.xxx.xx.xxx:xxxx,xxx.xxx.xx.xxx:xxxx,xxx.xxx.xx.xxx:xxxx --replication-factor 1 --partitions 1 --topic TOPIC_NAME_WITH_INDEX_SUFFIX_$c
        sleep 2
    done
}

. /run.config
createTopics $NUMBER_OF_RUNS
