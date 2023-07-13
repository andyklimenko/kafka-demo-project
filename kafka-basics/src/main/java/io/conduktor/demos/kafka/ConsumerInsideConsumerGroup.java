package io.conduktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerInsideConsumerGroup {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        final String groupID = "mu-java-application";
        final String topic = "demo_java";

        log.info("I'm a Kafka Consumer!");

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:29092");

        // create consumer configs
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupID);
        properties.setProperty("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, let's exit..");
                consumer.wakeup(); // will make a consumer to throw a WakeupException

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        consumer.subscribe(Arrays.asList(topic));

        /*
        * Our kafka broker has 3 partition, so if we say have 1 consumer it will receive the data from all 3 partitions
        *
        * BUT if we start one more consumer instance the kafka will REBALANCE and assign one partition from previous consumer instance to a new one
        * Launch of 3rd consumer will also trigger a REBALANCE and each of consumers will be assigned to only ONE unique partition to consume.
        * That's how we can consume partitions in parralel + make sure that each consumer won't access the data(from partition) the other consumer need
        *
        * If we run 4th consumer it will be assigned to one of existing partitions bot WON'T receive any data(the data will be consumed by another consumer that was assigned to the same partition first)
        *  BUT!!!!! if the previous consumer shuts down, we'll have rebalance and our new consumer will receive messages
        * */
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> r : records) {
                    log.info("Key: " + r.key() + ", Value: " + r.value());
                    log.info("Partition: " + r.partition() + " Offset: " + r.offset());
                }
            }
        } catch (WakeupException e) {
            log.info("consumer is starting to shutdown");
        } catch (Exception e) {
            log.error("unexpected exception", e);
        } finally {
            consumer.close();
            log.info("the consumer is now gracefully shutdown");
        }
    }
}
