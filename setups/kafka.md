# Apache Kafka Setup
```bash
# Download Kafka
wget https://downloads.apache.org/kafka/3.8.1/kafka_2.13-3.8.1.tgz
tar -xzf kafka_2.13-3.8.1.tgz
mv kafka_2.13-3.8.1 kafka
```

## Configure kafka for Kraft mode (no zookeeper)

### 1. Generate cluster ID
```bash
cd ~/kafka
bin/kafka-storage.sh random-uuid
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
```

### 4. Start kafka in Kraft mode
```bash
bin/kafka-server-start.sh config/kraft/server.properties
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

## Start with kafka
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