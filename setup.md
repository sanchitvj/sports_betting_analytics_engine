Here's a clear documentation format for setting up Kafka, Druid, and Grafana on an EC2 instance:

# Infrastructure Setup Guide

## Prerequisites
```bash
# EC2 Instance Requirements
- Instance Type: t2.xlarge (minimum)
- Storage: 100GB SSD
- OS: Ubuntu Server 22.04 LTS
- Memory: 16GB RAM
- vCPUs: 4

# Security Group Settings
Inbound Rules:
- SSH (22): Your IP
- Kafka (9092): 0.0.0.0/0
- Druid Console (8888): 0.0.0.0/0
- Grafana (3000): 0.0.0.0/0
```

## 1. Initial Setup
```bash
# Update system
sudo apt-get update
sudo apt-get upgrade -y

# Install Java
sudo apt-get install -y default-jdk
java -version

# Install wget and other utilities
sudo apt-get install -y wget curl net-tools
```

## 2. Apache Kafka Setup
```bash
# Download Kafka
wget https://downloads.apache.org/kafka/3.8.1/kafka_2.13-3.8.1.tgz
tar -xzf kafka_2.13-3.8.1.tgz
mv kafka_2.13-3.8.1 kafka

# Start ZooKeeper
cd kafka
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka Server
bin/kafka-server-start.sh config/server.properties
```

# Create topic
```bash
# Create game events topic
bin/kafka-topics.sh --create --topic weather.current.dc --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# Create analytics topic
bin/kafka-topics.sh --create --topic weather_analytics --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### stop all Kafka and ZooKeeper processes:
```bash
bin/kafka-server-stop.sh
bin/zookeeper-server-stop.sh
```

### Clean up Kafka and ZooKeeper data directories:
```bash
# Remove Kafka data
rm -rf /home/ubuntu/kafka/data/kafka/*

# Remove ZooKeeper data
rm -rf /home/ubuntu/kafka/data/zookeeper/*
rm -rf /tmp/zookeeper/
```

### Verify topics were created:
```bash
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

## 3. Apache Druid Setup
```bash
# Download Druid
wget https://dlcdn.apache.org/druid/27.0.0/apache-druid-27.0.0-bin.tar.gz
tar -xzf apache-druid-27.0.0-bin.tar.gz
mv apache-druid-27.0.0 druid

# Configure Druid
cd druid
vi conf/druid/single-server/micro-quickstart/_common/common.runtime.properties

# Add these configurations
druid.extensions.loadList=["druid-kafka-indexing-service"]
druid.zk.service.host=localhost
druid.metadata.storage.type=derby
druid.metadata.storage.connector.connectURI=jdbc:derby://localhost:1527/var/druid/metadata.db;create=true
druid.storage.type=local
druid.storage.storageDirectory=var/druid/segments

# Start Druid
./bin/start-micro-quickstart
```

## 4. Grafana Setup
```bash
# Install Grafana
sudo apt-get install -y apt-transport-https software-properties-common
wget -q -O - https://packages.grafana.com/gpg.key | sudo apt-key add -
echo "deb https://packages.grafana.com/oss/deb stable main" | sudo tee -a /etc/apt/sources.list.d/grafana.list
sudo apt-get update
sudo apt-get install grafana

# Start Grafana
sudo systemctl start grafana-server
sudo systemctl enable grafana-server

# Install Kafka plugin
sudo grafana-cli plugins install grafana-kafka-datasource
sudo systemctl restart grafana-server
```

## 5. Configure Data Pipeline

### Druid Ingestion
```json
{
  "type": "kafka",
  "dataSchema": {
    "dataSource": "sports_analytics",
    "timestampSpec": {
      "column": "processing_time",
      "format": "auto"
    },
    "dimensionsSpec": {
      "dimensions": [
        "game_id",
        "home_team_name",
        "away_team_name",
        "status_state",
        "venue_name"
      ]
    },
    "metricsSpec": [
      { "type": "doubleSum", "name": "home_team_score", "fieldName": "home_team_score" },
      { "type": "doubleSum", "name": "away_team_score", "fieldName": "away_team_score" }
    ]
  },
  "ioConfig": {
    "type": "kafka",
    "consumerProperties": {
      "bootstrap.servers": "localhost:9092"
    },
    "topic": "sports_analytics",
    "inputFormat": {
      "type": "json"
    }
  }
}
```

### Grafana Dashboard Setup
1. Access Grafana: `http://<EC2-Public-IP>:3000`
2. Default login: admin/admin
3. Add Data Source:
   - Type: Apache Druid
   - URL: http://localhost:8888
   - Access: Server (default)

## 6. Verify Setup
```bash
# Check Kafka
nc -zv localhost 9092

# Check Druid
curl http://localhost:8888/status

# Check Grafana
sudo systemctl status grafana-server
```

## 7. Monitoring
```bash
# View Kafka logs
tail -f kafka/logs/server.log

# View Druid logs
tail -f druid/log/coordinator.log

# View Grafana logs
sudo journalctl -u grafana-server -f
```

## 8. Troubleshooting
```bash
# Clear Kafka topics
kafka/bin/kafka-topics.sh --delete --topic sports_analytics \
    --bootstrap-server localhost:9092

# Reset Druid segments
rm -rf druid/var/druid/segments/*

# Restart all services
sudo systemctl restart grafana-server
./druid/bin/start-micro-quickstart
kafka/bin/kafka-server-start.sh config/server.properties
```

This setup provides:
1. Real-time data ingestion through Kafka
2. Fast analytics queries with Druid
3. Interactive dashboards in Grafana
4. Proper monitoring and troubleshooting
