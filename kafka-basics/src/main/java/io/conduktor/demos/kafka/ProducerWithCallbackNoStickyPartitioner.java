package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallbackNoStickyPartitioner {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka Producer!");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:29092");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        properties.setProperty("batch.size", "400"); // kafka's default batchSize is 16kB

        // set round-robin partitioner
        //properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());// don't use it in production

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer(properties);

        log.info("now let's send a way more messages and after each message we'll wait for 500ms");
        for (int j = 0; j<10; j++) {
            for (int i = 0; i<30; i++) {
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

                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        // producer will send all data and block until done - synchronous operation
        producer.flush();

        producer.close();

        log.info("messages are sending once a 0.5s, so kafka won't pack them into batches, we can see how they are distributed across partitions");
    }
}
