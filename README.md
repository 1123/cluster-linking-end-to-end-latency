# Cluster Linking Latency Measurement

This repository allows to measure end to end latency for

1. a producer writing data to a Kafka topic on a source cluster
2. the data being replicated via Confluent Cluster Linking to a destination cluster
3. the data being consumed from the destination cluster via an ordinary Kafka consumer

Latency is measured as the average difference in time from the data being
handed over to the Kafka producer and the time it is processed by the consumer application. 

When the producer sends the data to the source cluster, it includes a timestamp 
in the message payload. The consumer reads this timestamp and compares it 
to the current system time. Since both the producer and the consumer run on the same machine, 
clock skew is not an issue. 

## Prerequisites

* JDK 11 
* A recent version of Confluent Platform installed
* Linux or MacOS Operating System
* Apache Maven

## Running the Demo

* Follow the steps in `start.sh` to setup the Kafka clusters, 
  the cluster link, and the mirrored topics. 
* Run the Latency Junit Test in `kafka-latency-tests/org/example/kafkalatencytest/LatencyTest.java`

## Conclusions

* Latency of cluster links is measured to be at about 250 ms on average with default settings
* This can likely be improved by optimizing the settings



