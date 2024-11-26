# Apache Kafka Setup
```bash
# Download Kafka 3.7.1, versions 3.8.* are making issues with s3 sink connector so don't upgrade kafka
wget https://downloads.apache.org/kafka/3.7.1/kafka_2.13-3.7.1.tgz
tar -xzf kafka_2.13-3.7.1.tgz
mv kafka_2.13-3.7.1 kafka
```

## Configure kafka for Kraft mode (no zookeeper)

### 1. Generate cluster ID
```bash
mkdir -p /home/ubuntu/kafka/kraft-combined-logs

cd ~/kafka
bin/kafka-storage.sh random-uuid
# OR
# KAFKA_CLUSTER_ID=$(bin/kafka-storage.sh random-uuid)
# Save the output UUID
```

### 2. Create server.properties for KRaft
```bash
nano config/kraft/server.properties

# Add these configurations
node.id=1
process.roles=broker,controller
listeners=PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093
controller.listener.names=CONTROLLER
controller.quorum.voters=1@localhost:9093
inter.broker.listener.name=PLAINTEXT
advertised.listeners=PLAINTEXT://localhost:9092
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs=/home/ubuntu/kafka/kraft-combined-logs
```

### 3. Format storage directory
```bash
# Use the UUID you generated earlier
KAFKA_CLUSTER_ID="your-generated-uuid"
bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties
# OR
# bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties
```

### 4. Start kafka in Kraft mode
```bash
# -daemon to ignore the output
bin/kafka-server-start.sh config/kraft/server.properties
```
## S3 Connector Sink
Manually download AWS kafka s3 connect sink and put it in `~/kafka/connector/`.

1. Verify your connector files in right location
```bash
# Create plugins directory if it doesn't exist
mkdir -p ~/kafka/plugins

# Move the S3 connector files to plugins directory
mv ~/kafka/connectors/confluentinc-kafka-connect-s3-10.5.17/* ~/kafka/plugins/
```

2. Configure kafka connect standalone
```bash
mkdir -p config/connect

# Edit connect-standalone.properties
nano ~/kafka/config/connect-standalone.properties

# Add these settings
bootstrap.servers=localhost:9092
key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=false
value.converter.schemas.enable=false
offset.storage.file.filename=/tmp/connect.offsets
plugin.path=/home/ubuntu/kafka/plugins
# Important: This is the REST API port setting, when 8083 is occupied
listeners=HTTP://0.0.0.0:8084
rest.port=8084
```

3. Create S3 sink config
```bash
# Create config directory
mkdir -p ~/kafka/config/connect

# Create s3 sink config file
nano ~/kafka/config/connect/s3-sink.properties
```
```bash
# Add this configuration
name=s3-sink-sports
connector.class=io.confluent.connect.s3.S3SinkConnector
tasks.max=1

# Topics by sport
topics=nba_games_analytics,nba_odds_analytics,nba_news_analytics,nba_weather_analytics,\
       nfl_games_analytics,nfl_odds_analytics,nfl_news_analytics,nfl_weather_analytics,\
       cfb_games_analytics,cfb_odds_analytics,cfb_news_analytics,cfb_weather_analytics,\
       nhl_games_analytics,nhl_odds_analytics,nhl_news_analytics,nhl_weather_analytics

# S3 configuration
s3.bucket.name=raw-sp-data-aeb
s3.region=us-east-1
s3.part.size=5242880
flush.size=1000

# Storage settings
storage.class=io.confluent.connect.s3.storage.S3Storage
format.class=io.confluent.connect.s3.format.json.JsonFormat
partitioner.class=io.confluent.connect.storage.partitioner.TimeBasedPartitioner
timestamp.extractor=Record
partition.duration.ms=3600000
path.format='${topic}/year'=yyyy/'month'=MM/'day'=dd/'hour'=HH
locale=en-US
timezone=UTC

schema.compatibility=NONE
behavior.on.null.values=ignore
file.delim=+
```

4. Start Kafka Connect
```bash
# First start kafka in kraft mode then start connector
 
bin/connect-standalone.sh config/connect-standalone.properties config/connect/s3-sink.properties
```
```bash
# Check connector status
curl -s localhost:8083/connectors/s3-sink-sports/status | jq
```

## Legacy Setup (with Zookeeper)
### First time only
1. Create ZooKeeper service file:
```bash
sudo nano /etc/systemd/system/zookeeper.service

# Set permissions for Kafka directory
sudo chown -R ubuntu:ubuntu /home/ubuntu/kafka
chmod -R 755 /home/ubuntu/kafka
```

2. Add the following content:
```ini
[Unit]
Description=Apache Zookeeper Server
Documentation=http://zookeeper.apache.org
Requires=network.target remote-fs.target
After=network.target remote-fs.target

[Service]
Type=simple
User=ubuntu
Environment="JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64"
ExecStart=/home/ubuntu/kafka/bin/zookeeper-server-start.sh /home/ubuntu/kafka/config/zookeeper.properties
ExecStop=/home/ubuntu/kafka/bin/zookeeper-server-stop.sh
Restart=on-abnormal

[Install]
WantedBy=multi-user.target
```

```bash
# Verify ZooKeeper config
vi /home/ubuntu/kafka/config/zookeeper.properties
# Ensure these settings:
dataDir=/tmp/zookeeper
clientPort=2181
maxClientCnxns=0
```

3. Create Kafka service file:
```bash
sudo nano /etc/systemd/system/kafka.service
```

4. Add the following content:
```ini
[Unit]
Description=Apache Kafka Server
Documentation=http://kafka.apache.org/documentation.html
Requires=zookeeper.service
After=zookeeper.service

[Service]
Type=simple
User=ubuntu
Environment="JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64"
ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
ExecStop=/opt/kafka/bin/kafka-server-stop.sh
Restart=on-abnormal

[Install]
WantedBy=multi-user.target
```

```bash
# Edit zookeeper.properties
nano /home/ubuntu/kafka/config/zookeeper.properties

# Add/modify these settings
dataDir=/home/ubuntu/kafka/data/zookeeper
clientPort=2181
maxClientCnxns=0
admin.enableServer=false

nano /home/ubuntu/kafka/config/server.properties
# Ensure these settings:
broker.id=0
listeners=PLAINTEXT://localhost:9092
log.dirs=/tmp/kafka-logs
zookeeper.connect=localhost:2181
```

```bash
# Edit server.properties
nano /home/ubuntu/kafka/config/server.properties

# Add/modify these settings
broker.id=0
listeners=PLAINTEXT://localhost:9092
log.dirs=/tmp/kafka-logs
zookeeper.connect=localhost:2181
```

5. Reload systemd and start services:
```bash
# Reload systemd
sudo systemctl daemon-reload

# Start ZooKeeper
sudo systemctl start zookeeper

# Verify ZooKeeper is running
sudo systemctl status zookeeper

# Start Kafka
sudo systemctl start kafka

# Verify Kafka is running
sudo systemctl status kafka

# Enable services to start on boot
sudo systemctl enable zookeeper
sudo systemctl enable kafka
```

6. Verify the ports are listening:
```bash
# Check if ZooKeeper port is listening
netstat -tulpn | grep 2181

# Check if Kafka port is listening
netstat -tulpn | grep 9092
```

### Start with kafka
1. Start services
```bash
# Start ZooKeeper, remove -daemon if you want to see output
bin/zookeeper-server-start.sh -daemon config/zookeeper.properties

# Start Kafka Server
bin/kafka-server-start.sh -daemon config/server.properties
```

2. Create topics
```bash
# Create game events topic
bin/kafka-topics.sh --create --topic weather.current.dc --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# Create analytics topic
bin/kafka-topics.sh --create --topic weather_analytics --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

3. List topics
```bash
# Verify Kafka is running
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

## Cleaning
1. Stop all Kafka and ZooKeeper processes:
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
rm -rf /opt/druid/var/

rm -rf /home/ubuntu/kafka/data/zookeeper
rm -rf /tmp/kafka-logs/
rm -rf /tmp/zookeeper/
```