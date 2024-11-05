resource "aws_instance" "kafka_broker" {
  count                  = var.broker_count
  ami                    = var.kafka_ami
  instance_type          = var.instance_type
  subnet_id              = var.subnet_id
  vpc_security_group_ids = [var.security_group_id]
  key_name              = var.key_name

  user_data = templatefile("${path.module}/scripts/kafka_setup.sh", {
    broker_id = count.index
    zookeeper_connect = var.zookeeper_connect
  })

  tags = {
    Name = "kafka-broker-${count.index}"
    Environment = var.environment
  }
}

terraform {
  backend "s3" {
    bucket         = ""
    key            = "state_file/dynamodb.tfstate" # modify
    region         = ""
    encrypt        = true
    dynamodb_table = ""
  }
}