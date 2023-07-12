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