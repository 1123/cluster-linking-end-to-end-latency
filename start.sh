#!/bin/bash

rm -rf logs
mkdir -p logs

export JAVA_HOME=$(/usr/libexec/java_home -v11)

set -e -u

$CONFLUENT_HOME/bin/zookeeper-server-start zookeeper-src.properties > logs/zk-src.log 2>&1 &
$CONFLUENT_HOME/bin/kafka-server-start server-src.properties > logs/kafka-src.log 2>&1 &

$CONFLUENT_HOME/bin/zookeeper-server-start zookeeper-dst.properties > logs/zk-dst.log 2>&1 &
$CONFLUENT_HOME/bin/kafka-server-start server-dst.properties > logs/kafka-dst.log 2>&1 &

kafka-topics --create \
  --topic demo \
  --bootstrap-server localhost:9092

kafka-topics --list --bootstrap-server localhost:9092

kafka-topics --describe --topic demo --bootstrap-server localhost:9092

kafka-console-producer \
  --topic demo \
  --bootstrap-server localhost:9092

kafka-producer-perf-test \
  --producer-props bootstrap.servers=localhost:9092 \
  --topic demo \
  --throughput 100 \
  --record-size 1000 \
  --num-records 1000

kafka-console-consumer --topic demo --from-beginning --bootstrap-server localhost:9092

kafka-cluster-links \
  --bootstrap-server localhost:9093 \
  --create \
  --link demo-link \
  --config bootstrap.servers=localhost:9092,replica.fetch.wait.max.ms=100,replica.fetch.min.bytes=0

kafka-mirrors --create \
  --mirror-topic demo \
  --link demo-link \
  --bootstrap-server localhost:9093

kafka-console-consumer \
  --topic demo \
  --from-beginning \
  --bootstrap-server localhost:9093

kafka-replica-status \
  --topics demo \
  --include-linked \
  --bootstrap-server localhost:9093

kafka-console-producer --topic demo --bootstrap-server localhost:9093

kafka-configs --describe \
  --topic demo \
  --bootstrap-server localhost:9093

kafka-configs --alter --topic demo --add-config retention.ms=123456890 --bootstrap-server localhost:9092

kafka-configs --describe --topic demo --bootstrap-server localhost:9093

# Cleanup

kafka-topics --delete \
  --topic demo \
  --bootstrap-server localhost:9092

kafka-topics --delete \
  --topic demo \
  --bootstrap-server localhost:9093

kafka-cluster-links \
  --delete \
  --link demo-link \
  --bootstrap-server localhost:9093
