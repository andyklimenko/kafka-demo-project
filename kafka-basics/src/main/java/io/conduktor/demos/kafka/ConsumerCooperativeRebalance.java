package io.conduktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerCooperativeRebalance {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        final String groupID = "my-java-application";
        final String topic = "demo_java";

        log.info("I'm a Kafka Consumer!");

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:29092");

        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupID);
        properties.setProperty("auto.offset.reset", "earliest");

        //default partition.assignment.strategy value is [class org.apache.kafka.clients.consumer.RangeAssignor, class org.apache.kafka.clients.consumer.CooperativeStickyAssignor]

        // let's override it
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());
        // now partition.assignment.strategy is [org.apache.kafka.clients.consumer.CooperativeStickyAssignor]
        /* and some new stuff appeared in the output which will show partitions assigned to our consumer(name it consumerA)
        	Assigned partitions:                       [demo_java-0, demo_java-1, demo_java-2]
	        Current owned partitions:                  []
	        Added partitions (assigned - owned):       [demo_java-0, demo_java-1, demo_java-2]
	        Revoked partitions (owned - assigned):     []
         */

        /*
        * If we run one more instance(say consumerB of that consumer it will show that one partition was taken from consumerA instance and assigned here:
        * 	Assigned partitions:                       [demo_java-2]
	        Current owned partitions:                  []
	        Added partitions (assigned - owned):       [demo_java-2]
	        Revoked partitions (owned - assigned):     []

        * And in the meantime our consumerA will show updated list of assigned partitions:
        	Assigned partitions:                       [demo_java-0, demo_java-1]
	        Current owned partitions:                  [demo_java-0, demo_java-1]
	        Added partitions (assigned - owned):       []
	        Revoked partitions (owned - assigned):     []
        * */

        /*
         * And with 3rd consumer running(consumerC) we'll see that one partition was un-assigned from consumerA:
         	Assigned partitions:                       [demo_java-0]
	        Current owned partitions:                  [demo_java-0]
	        Added partitions (assigned - owned):       []
	        Revoked partitions (owned - assigned):     []
	     * and added to consumerC:
	        Assigned partitions:                       [demo_java-1]
	        Current owned partitions:                  []
	        Added partitions (assigned - owned):       [demo_java-1]
	        Revoked partitions (owned - assigned):     []
	     * BUT consumerB kept working uninterrupted:
	     	Assigned partitions:                       [demo_java-2]
	        Current owned partitions:                  [demo_java-2]
	        Added partitions (assigned - owned):       []
	        Revoked partitions (owned - assigned):     []
         *
         */

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, let's exit..");
                consumer.wakeup();

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
