kafka-topics.bat --zookeeper 127.0.0.1:2181 --topic first_topic --create --partitions 3 --replication-factor 1
// kafka topic connects to zookeeper
// cannot create topic with a replication-factor greater than the number of brokers available.

kafka-topics.bat --zookeeper 127.0.0.1:2181 --list

kafka-topics.bat --zookeeper 127.0.0.1:2181 --topic second_topic --create --partitions 6 --replication-factor 1

kafka-topics.bat --zookeeper 127.0.0.1:2181 --topic second_topic --delete

kafka-topics.bat --zookeeper 127.0.0.1:2181 --topic new_topic --describe

kafka-console-producer.bat --broker-list 127.0.0.1:9092 --topic first_topic

kafka-console-producer.bat --broker-list 127.0.0.1:9092 --topic first_topic --producer-property acks=all
// topic that doesn't exist will come with a warning AT FIRST (LEADER_NOT_AVAILABLE)
// then creates a new topic

*** server.properties ***
num.partitions=3 // new topics will get 3 partitions by default
default.replication.factor // replication-factor
***

kafka-console-consumer.bat --bootstrap-server 127.0.0.1:9092 --topic first_topic
// only reads new msgs after starting

kafka-console-consumer.bat --bootstrap-server 127.0.0.1:9092 --topic first_topic --from-beginning
// reads everything from beginning

kafka-console-consumer.bat --bootstrap-server 127.0.0.1:9092 --topic first_topic --group my-first-application

kafka-consumer-groups.bat --bootstrap-server 127.0.0.1:9092 --list
// if no group was specified, some random group gets generated

kafka-consumer-groups.bat --bootstrap-server 127.0.0.1:9092 --describe --group my-second-application
// shows current offset, log-end offset, lag etc for each topic and partition

kafka-consumer-groups.bat --bootstrap-server localhost:9092 --group my-first-application --reset-offsets --to-earliest --execute --topic first_topic

kafka-consumer-groups.bat --bootstrap-server localhost:9092 --group my-first-application --reset-offsets --shift-by -2 --execute --topic first_topic

// for key, value format
kafka-console-producer --broker-list 127.0.0.1:9092 --topic first_topic --property parse.key=true --property key.separator=,
kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic first_topic --from-beginning --property print.key=true --property key.separator=,
