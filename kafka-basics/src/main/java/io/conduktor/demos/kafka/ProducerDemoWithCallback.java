package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka Producer!");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:29092");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer(properties);


        for (int i = 0; i<10; i++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord("demo_java", "hello world" + i);
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    // executes every time a record successfully sent or an exception is thrown
                    if (e != null) {
                        log.error("Error while producing", e);
                        return;
                    }

                    log.info("Received new metadata \n" +
                            "Topic: " + metadata.topic() + "\n" +
                            "Partition: " + metadata.partition() + "\n" +
                            "Offset: " + metadata.offset() + "\n" +
                            "Timestamp: " + metadata.timestamp() + "\n" +
                            "Offset: " + metadata.offset() + "\n");
                }
            });
        }

        log.info("the producer is smart enough to pack messages we sequentially send into a single batch and send them at once \n" +
                "that's why all messages looks sent to the same partition, we call it STICKY PARTITIONER");

        // producer will send all data and block until done - synchronous operation
        producer.flush();

        producer.close();
    }
}
