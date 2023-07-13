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

public class ConsumerWithGracefulShutdown {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        final String groupID = "my-java-application";
        final String topic = "demo_java";

        log.info("I'm a Kafka Consumer!");

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:29092");

        // create consumer configs
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupID);

        // supported values - [none | earliest | latest]
        // none     - if we don't have any existing consumer group - we fail
        // earliest - read from the beginning of the topic(similar to --from-beginning option passed to kafka-console-consumer.sh
        // latest   - we want to read only new messages
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

        try {
            while (true) {
                log.info("Polling..");
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
            consumer.close(); // also commits the offsets
            log.info("the consumer is now gracefully shutdown");
        }
    }
}
