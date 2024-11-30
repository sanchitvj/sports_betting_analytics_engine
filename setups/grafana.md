# Grafana Setup

### Install Grafana
```bash
sudo apt-get install -y apt-transport-https software-properties-common
wget -q -O - https://packages.grafana.com/gpg.key | sudo apt-key add -
echo "deb https://packages.grafana.com/oss/deb stable main" | sudo tee -a /etc/apt/sources.list.d/grafana.list
sudo apt-get update
sudo apt-get install grafana
```

### Start Grafana
```bash
sudo systemctl start grafana-server
sudo systemctl enable grafana-server
```

### Install Kafka plugin
```bash
sudo grafana-cli plugins install grafana-kafka-datasource
sudo systemctl restart grafana-server
```
