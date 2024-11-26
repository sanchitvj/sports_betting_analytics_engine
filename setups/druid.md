# Apache Druid Setup

1. Download Druid
```bash
wget https://dlcdn.apache.org/druid/27.0.0/apache-druid-27.0.0-bin.tar.gz
tar -xzf apache-druid-27.0.0-bin.tar.gz
mv apache-druid-27.0.0 druid
```

2. Configure Druid
```bash
# Edit common runtime properties
nano ~/druid/conf/druid/single-server/micro-quickstart/_common/common.runtime.properties

# Add these settings
druid.extensions.loadList=["druid-kafka-indexing-service"]
druid.zk.service.host=localhost
druid.metadata.storage.type=derby
druid.metadata.storage.connector.connectURI=jdbc:derby://localhost:1527/var/druid/metadata.db;create=true
druid.storage.type=local
druid.storage.storageDirectory=var/druid/segments
druid.indexer.logs.directory=var/druid/indexing-logs
```

3. Start Druid
```bash
./bin/start-micro-quickstart
```

## Cleaning
```bash
# Stop all processes
cd ~/druid
./bin/stop-druid.sh

# Kill any remaining processes
sudo pkill -f druid

# Remove existing data
rm -rf ~/druid/var/*
rm -rf /tmp/druid-tmp/

# Create fresh directories
mkdir -p ~/druid/var/druid/segments
mkdir -p ~/druid/var/druid/indexing-logs
mkdir -p /tmp/druid-tmp
```

## S3 Storage Config
```bash
nano ~/druid/conf/druid/single-server/micro-quickstart/_common/common.runtime.properties

druid.extensions.loadList=["druid-s3-extensions"]

# Add/modify these settings
druid.storage.type=s3
druid.storage.bucket=hist-sp-data-aeb
druid.storage.baseKey=druid/segments

# S3 Configuration
druid.s3.accessKey=your-access-key
druid.s3.secretKey=your-secret-key
druid.s3.protocol=http
druid.s3.region=us-east-1

# Historical Deep Storage
druid.segmentCache.locations=[{"path":"/home/ubuntu/druid/var/druid/segment-cache","maxSize":130000000000}]
druid.segmentCache.numBootstrapThreads=1

# Indexing Service Storage
druid.indexer.logs.type=s3
druid.indexer.logs.s3Bucket=hist-sp-data-aeb
druid.indexer.logs.s3Prefix=druid/indexing-logs
```


## Cleaning, kafka-druid schema change

To delete a Kafka topic and ensure Druid ingests fresh data with the new schema, follow these steps:

1. Delete the Kafka topic:
```bash
kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic nba_odds_analytics
```

2. Delete the Druid supervisor:
```bash
curl -X POST http://localhost:8081/druid/indexer/v1/supervisor/nba_odds_analytics/terminate
```

3. Delete the Druid datasource:
```bash
curl -X POST http://localhost:8081/druid/coordinator/v1/datasources/nba_odds_analytics --data 'kill=true&interval=1000/3000'
```

4. Recreate the Kafka topic:
```bash
kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic nba_odds_analytics
```

## Optional (CAUTION: NOT RECOMMENDED)
```bash
# Edit JVM config
nano ~/druid/conf/druid/single-server/micro-quickstart/_common/jvm.config

# Add these lines
-Xms512m
-Xmx512m
-XX:MaxDirectMemorySize=1g
-XX:+UseG1GC
```
```bash
# If zk issue

# Edit common.runtime.properties
nano ~/druid/conf/druid/single-server/micro-quickstart/_common/common.runtime.properties

# Add/modify these settings
druid.zk.service.host=localhost
druid.zk.service.port=2181
druid.zk.paths.base=/druid

# Edit micro-quickstart.conf
nano ~/druid/conf/supervise/single-server/micro-quickstart.conf

# Comment out or remove the zk line
# !p10 zk conf

# Should look like this:
!p10 coordinator-overlord conf
!p10 broker conf
!p10 historical conf
!p10 middleManager conf
!p10 router conf

```
