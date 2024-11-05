data "aws_ami" "ubuntu_pro" {
  most_recent = true
  owners      = ["amazon"] # Canonical's AWS account ID

  filter {
    name   = "name"
    values = ["ubuntu-pro-server/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-pro-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

resource "aws_security_group" "kafka_airflow_sg" {
  name        = "kafka-airflow-sg"
  description = "Security group for Kafka and Airflow services"
  vpc_id      = var.vpc_id

  # SSH access
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr_blocks]
  }

  # Kafka Broker
  ingress {
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  # Kafka Connect
  ingress {
    from_port   = 8083
    to_port     = 8083
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  # Zookeeper
  ingress {
    from_port   = 2181
    to_port     = 2181
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  # Airflow Webserver
  ingress {
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr_blocks]
  }

  # Airflow Flower
  ingress {
    from_port   = 5555
    to_port     = 5555
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  # Allow all outbound traffic
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "kafka-airflow-sg"
  }
}

resource "aws_instance" "kafka_airflow_instance" {
  ami           = data.aws_ami.ubuntu_pro.id
  instance_type = "t3a.medium"

  subnet_id                   = var.subnet_id
  vpc_security_group_ids     = [aws_security_group.kafka_airflow_sg.id]
  associate_public_ip_address = true
  key_name                   = var.key_pair_name
  iam_instance_profile       = var.iam_role_name

  root_block_device {
    volume_type = "gp3"
    volume_size = 64
    iops        = 3000
    throughput  = 125
  }

  # Use template file for user data
  user_data = templatefile("${path.module}/scripts/kafka_setup.sh", {
    kafka_data_dir     = "/data/kafka"
    kafka_version      = var.kafka_version
    zookeeper_version  = var.zookeeper_version
    broker_id          = "0"
    kafka_heap_opts    = "-Xmx1G -Xms1G"
    zookeeper_host     = "localhost:2181"
  })
  tags = {
    Name = "kafka-airflow-server"
  }
}


output "instance_id" {
  value = aws_instance.kafka_airflow_instance.id
}

output "public_ip" {
  value = aws_instance.kafka_airflow_instance.public_ip
}

output "private_ip" {
  value = aws_instance.kafka_airflow_instance.private_ip
}

output "security_group_id" {
  description = "ID of the created security group"
  value       = aws_security_group.kafka_airflow_sg.id
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