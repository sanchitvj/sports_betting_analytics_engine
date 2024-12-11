# Infrastructure Setup Guide

## Prerequisites
```bash
# EC2 Instance Requirements
- Instance Type: t3a.2xlarge
- Storage: 100GB SSD
- OS: Ubuntu Pro
- Memory: 32GB RAM
- vCPUs: 8

# Security Group Settings
Inbound Rules:
- SSH (22): Your EC2 instance IP
- Kafka (9092): 0.0.0.0/0
- Zookeeper (2181): 0.0.0.0/0
- Druid Console (8888): 0.0.0.0/0
- Grafana (3000): 0.0.0.0/0
- Airflow (8090): 0.0.0.0/0
```

## Initial Setup
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

For individual service setup check their markdowns.