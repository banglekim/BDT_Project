# BDT_Project
Final project for Big Data Techniologies course using Spark Streaming, HBase, Kafka, Twitter API...

# Instructions

- Start Kafka
KAFKA_HOME=/home/cloudera/kafka_2.12-2.6.0; export KAFKA_HOME
//Start Zookeeper server
$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties
//Start Kafka server
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties

- Create a topic
//Create a topic
$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic KafkaData

- Test Kafka with Kafka producer and consumer
//Send some messages
$KAFKA_HOME/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic KafkaData 
//Start a consumer
$KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic KafkaData --from-beginning

- Start consumer application
spark-submit --class "bangle.cs523.SparkStreamProj.App" --master yarn /home/cloudera/FinalProject/SparkStreamProj/target/SparkStreamProj-0.0.1-SNAPSHOT.jar

- Start producer application
spark-submit --class "bangle.cs523.KafkaProducer.App" --master yarn /home/cloudera/FinalProject/KafkaProducer/target/KafkaProducer-0.0.1-SNAPSHOT.jar

- Check Hbase for result
