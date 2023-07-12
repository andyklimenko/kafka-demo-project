# Short summary of kafka beginner course 
[Original udemy course](https://www.udemy.com/course/apache-kafka/)

## Setup local env
```shell
cd infra
docker-compose up
```

## Create topic with required amount of partitions
```shell
kafka-topics.sh --bootstrap-server localhost:29092 --topic demo_java --create --partitions 3
```

## Consume that topic while running the producers
```shell
kafka-console-consumer.sh --bootstrap-server localhost:29092 --topic demo_java --from-beginning
```

# Index

1. [Produce first message](./kafka-basics/src/main/java/io/conduktor/demos/kafka/ProducerDemo.java)
2. [Producer with callback with sticky partitioning](./kafka-basics/src/main/java/io/conduktor/demos/kafka/ProducerDemoWithCallback.java)
3. [Producer with callback with no sticky partitioning](./kafka-basics/src/main/java/io/conduktor/demos/kafka/ProducerWithCallbackNoStickyPartitioner.java)
4. [Producer with message keys](./kafka-basics/src/main/java/io/conduktor/demos/kafka/ProducerWithKeys.java)