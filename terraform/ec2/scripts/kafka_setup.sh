# Extract Kafka (assuming you're in /home/ubuntu/kafka directory)
tar -xzf kafka_2.13-3.9.0.tgz
mv kafka_2.13-3.9.0/* .
rm -rf kafka_2.13-3.9.0 kafka_2.13-3.9.0.tgz

# Create data directories
mkdir -p data/zookeeper
mkdir -p data/kafka


# Backup original configs
cp config/server.properties config/server.properties.bak
cp config/zookeeper.properties config/zookeeper.properties.bak

# Update Zookeeper properties
tee config/zookeeper.properties <<EOF
dataDir=/home/ubuntu/kafka/data/zookeeper
clientPort=2181
maxClientCnxns=0
admin.enableServer=false
EOF

# Update Kafka properties
tee config/server.properties <<EOF
broker.id=0
listeners=PLAINTEXT://0.0.0.0:9092
advertised.listeners=PLAINTEXT://$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4):9092
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
log.dirs=/home/ubuntu/kafka/data/kafka
num.partitions=3
num.recovery.threads.per.data.dir=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
log.retention.hours=168
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000
zookeeper.connect=localhost:2181
zookeeper.connection.timeout.ms=18000
EOF


# Create Zookeeper service file
sudo tee /etc/systemd/system/zookeeper.service <<EOF
[Unit]
Description=Apache Zookeeper Server
Documentation=http://zookeeper.apache.org
Requires=network.target remote-fs.target
After=network.target remote-fs.target

[Service]
Type=simple
User=ubuntu
Environment="JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64"
ExecStart=/home/ubuntu/kafka/bin/zookeeper-server-start.sh /home/ubuntu/kafka/config/zookeeper.properties
ExecStop=/home/ubuntu/kafka/bin/zookeeper-server-stop.sh
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

# Create Kafka service file
sudo tee /etc/systemd/system/kafka.service <<EOF
[Unit]
Description=Apache Kafka Server
Documentation=http://kafka.apache.org/documentation.html
Requires=zookeeper.service
After=zookeeper.service

[Service]
Type=simple
User=ubuntu
Environment="JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64"
ExecStart=/home/ubuntu/kafka/bin/kafka-server-start.sh /home/ubuntu/kafka/config/server.properties
ExecStop=/home/ubuntu/kafka/bin/kafka-server-stop.sh
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF


# Reload systemd
sudo systemctl daemon-reload

# Start Zookeeper
sudo systemctl start zookeeper
sudo systemctl status zookeeper

# If Zookeeper starts successfully, start Kafka
sudo systemctl start kafka
sudo systemctl status kafka

# Enable services to start on boot
sudo systemctl enable zookeeper
sudo systemctl enable kafka


# Create a test topic
bin/kafka-topics.sh --create \
    --bootstrap-server localhost:9092 \
    --topic test \
    --partitions 1 \
    --replication-factor 1

# List topics
bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Start a console producer (in one terminal)
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test

# Start a console consumer (in another terminal)
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning