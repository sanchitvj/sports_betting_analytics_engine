#!/bin/bash
set -e

# Update system
sudo apt-get update
sudo apt-get upgrade -y

# Install Java
sudo apt-get install -y openjdk-11-jdk

# Create directories
sudo mkdir -p ${kafka_data_dir}
sudo mkdir -p /data/airflow

# Install Kafka
cd /opt
wget "https://downloads.apache.org/kafka/${kafka_version}/kafka_2.13-${kafka_version}.tgz"
tar -xzf "kafka_2.13-${kafka_version}.tgz"
sudo mv "kafka_2.13-${kafka_version}" /opt/kafka
rm "kafka_2.13-${kafka_version}.tgz"

# Configure Kafka
cat << EOF > /opt/kafka/config/server.properties
broker.id=${broker_id}
listeners=PLAINTEXT://0.0.0.0:9092
advertised.listeners=PLAINTEXT://$(curl -s http://169.254.169.254/latest/meta-data/public-hostname):9092
log.dirs=${kafka_data_dir}
num.partitions=3
default.replication.factor=1
log.retention.hours=168
zookeeper.connect=${zookeeper_host}
EOF

# Create Kafka systemd service
cat << EOF > /etc/systemd/system/kafka.service
[Unit]
Description=Apache Kafka Server
Documentation=http://kafka.apache.org/documentation.html
Requires=zookeeper.service
After=zookeeper.service

[Service]
Type=simple
Environment="KAFKA_HEAP_OPTS=${kafka_heap_opts}"
ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
ExecStop=/opt/kafka/bin/kafka-server-stop.sh
Restart=on-abnormal

[Install]
WantedBy=multi-user.target
EOF

# Install and configure Zookeeper
cd /opt
wget "https://downloads.apache.org/zookeeper/zookeeper-${zookeeper_version}/apache-zookeeper-${zookeeper_version}-bin.tar.gz"
tar -xzf "apache-zookeeper-${zookeeper_version}-bin.tar.gz"
sudo mv "apache-zookeeper-${zookeeper_version}-bin" /opt/zookeeper
rm "apache-zookeeper-${zookeeper_version}-bin.tar.gz"

# Configure Zookeeper
cp /opt/zookeeper/conf/zoo_sample.cfg /opt/zookeeper/conf/zoo.cfg

# Create Zookeeper systemd service
cat << EOF > /etc/systemd/system/zookeeper.service
[Unit]
Description=Apache Zookeeper Server
Documentation=http://zookeeper.apache.org
Requires=network.target
After=network.target

[Service]
Type=simple
ExecStart=/opt/zookeeper/bin/zkServer.sh start-foreground
ExecStop=/opt/zookeeper/bin/zkServer.sh stop
Restart=on-abnormal

[Install]
WantedBy=multi-user.target
EOF

# Set correct permissions
sudo chown -R ubuntu:ubuntu /data
sudo chown -R ubuntu:ubuntu /opt/kafka
sudo chown -R ubuntu:ubuntu /opt/zookeeper

# Start services
sudo systemctl daemon-reload
sudo systemctl enable zookeeper
sudo systemctl enable kafka
sudo systemctl start zookeeper
sudo systemctl start kafka