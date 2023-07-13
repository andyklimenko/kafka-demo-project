package io.conduktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
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
        consumer.subscribe(Arrays.asList(topic));

        while (true) {
            /*
             * we can break this loop only via terminating the whole consumer process.
             * Because of that when consumer will be restarted next time it'll need way more time to start polling messages.
             * So the graceful shutdown is highly required to have.
             */
            log.info("Polling..");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> r : records) {
                log.info("Key: " + r.key() + ", Value: " + r.value());
                log.info("Partition: " + r.partition() + " Offset: " + r.offset());
            }
        }

        /*
        * The consumer is smart enough to consume the data in batches(up to 1Mb), so no need to worry about performance while polling.
        * */
    }
}
