# sports_betting_analytics_engine

The error shows a cluster ID mismatch in Kafka's metadata. Here's how to fix it:

1. First, stop all Kafka and ZooKeeper processes:
```bash
bin/kafka-server-stop.sh
bin/zookeeper-server-stop.sh
```

2. Clean up Kafka and ZooKeeper data directories:
```bash
# Remove Kafka data
rm -rf /home/ubuntu/kafka/data/kafka/*

# Remove ZooKeeper data
rm -rf /home/ubuntu/kafka/data/zookeeper/*
rm -rf /tmp/zookeeper/
```

3. Start fresh with ZooKeeper:
```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
```

4. In a new terminal, start Kafka server:
```bash
bin/kafka-server-start.sh config/server.properties
```

5. Create required topics:
```bash
# Create game events topic
bin/kafka-topics.sh --create --topic weather.current.dc --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# Create analytics topic
bin/kafka-topics.sh --create --topic weather_analytics --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

```

6. Verify topics were created:
```bash
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

7. If needed, update server.properties:
```properties
# In config/server.properties
log.dirs=/home/ubuntu/kafka/data/kafka
zookeeper.connect=localhost:2181
```
