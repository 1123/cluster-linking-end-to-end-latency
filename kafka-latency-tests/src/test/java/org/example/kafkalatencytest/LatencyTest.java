package org.example.kafkalatencytest;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This test helps to measure average end-to-end latency of a messages sent from one Cluster via Cluster Linking
 * to another Kafka Cluster.
 *
 * Producer --> Cluster1 ---cluster-link---> Cluster2 --> Consumer
 *
 * Each message is given a timestamp by the producer. Latency is computed by the consumer by
 * subtracting the producer timestamp from the current time.
 *
 * Here is some sample output:
 *
 [Thread-0] INFO org.example.kafkalatencytest.TestConsumer - finished: total latency 2538405
 [Thread-0] INFO org.example.kafkalatencytest.TestConsumer - average latency: 253.8405
 *
 * With a local installation of Confluent Kafka 7.2, a number of 10000 messages, sleep time of 1ms between
 * messages, consumer start up sleep time of 1000ms, the average latency of a round trip is
 * at about 255ms. This can probably be tuned to be much slower with the appropriate c luster linking configuration
 * parameters.
 *
 * When consuming the same message from the first cluster, latency is at about 1 ms. Hence, cluster linking introduces
 * an additional latency of about 250 ms, when no further tuning has been done.
 */

@Slf4j
public class LatencyTest {

    public static final String TOPIC = "demo";
    public static final int NUM_RECORDS = 10000;
    private static final long PRODUCER_SLEEP = 1L;
    private static final long CONSUMER_STARTUP_SLEEP = 1000L;

    @SneakyThrows
    public Properties producerProperties() {
        String propFileName = "producer.properties";
        Properties properties = new Properties();

        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName)){
            if (inputStream != null) {
                properties.load(inputStream);
            } else {
                throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
            }
        }
        return properties;
    }

    /**
     * Before the test, we need to set up a fresh topic in the source cluster, and a fresh mirror of the topic
     * in the destination cluster:
     *
     * kafka-topics --create --topic demo --bootstrap-server localhost:9092
     * kafka-cluster-links --bootstrap-server localhost:9093 \
     *   --create --link demo-link --config bootstrap.servers=localhost:9092
     * kafka-mirrors --create \
     *   --mirror-topic demo \
     *   --link demo-link \
     *   --bootstrap-server localhost:9093
     *
     * Delete the topics after the test:
     *
     * Cleanup:
     * kafka-topics --delete \
     *  --topic demo \
     *  --bootstrap-server localhost:9092
     *
     * kafka-topics --delete \
     *  --topic demo \
     *  --bootstrap-server localhost:9093
     **/

    @Test
    public void test() throws InterruptedException {
        log.info("Testing on topic {}", TOPIC);
        Thread consumerThread = new Thread(new TestConsumer());
        consumerThread.start();
        Thread.sleep(CONSUMER_STARTUP_SLEEP); // give the consumer some time to start.
        KafkaProducer<Integer, Long> kafkaProducer =
                new KafkaProducer<>(producerProperties());
        for (long i = 0; i < NUM_RECORDS + 100; i++) {
            Thread.sleep(PRODUCER_SLEEP);
            kafkaProducer.send(new ProducerRecord<>(TOPIC, (int) i, System.currentTimeMillis()));
        }
        kafkaProducer.close();
        consumerThread.join();
    }
}

@Slf4j
class TestConsumer implements Runnable {

    @SneakyThrows
    private Properties consumerProperties() {
        String propFileName = "consumer.properties";
        Properties properties = new Properties();

        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName)){
            if (inputStream != null) {
                properties.load(inputStream);
            } else {
                throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
            }
        }
        return properties;
    }

    @SneakyThrows
    @Override
    public void run() {
        KafkaConsumer<Integer, Long> kafkaConsumer =
                new KafkaConsumer<>(consumerProperties());
        kafkaConsumer.subscribe(Set.of(LatencyTest.TOPIC));
        AtomicInteger received = new AtomicInteger();
        AtomicLong totalLatency = new AtomicLong();
        while (received.get() < LatencyTest.NUM_RECORDS) {
            ConsumerRecords<Integer, Long> result = kafkaConsumer.poll(Duration.ofHours(1));
            result.iterator().forEachRemaining(
                    record -> {
                        //log.info("System.currentTimeMillis() - record.value(): {} - {} = {}",
                        //        System.currentTimeMillis(),
                        //        record.value(),
                        //        System.currentTimeMillis() - record.value());
                        received.getAndIncrement();
                        totalLatency.getAndAdd(System.currentTimeMillis() - record.value());
                    }
            );
        }
        log.info("finished: total latency {}", totalLatency.get());
        log.info("average latency: {}", totalLatency.get() / (LatencyTest.NUM_RECORDS + 0.0));
        kafkaConsumer.close();
    }
}
