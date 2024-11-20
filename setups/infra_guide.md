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
- Zookeeper (2181): 0.0.0.0/0
- Druid Console (8888): 0.0.0.0/0
- Grafana (3000): 0.0.0.0/0
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
