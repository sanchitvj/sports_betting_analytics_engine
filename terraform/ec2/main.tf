# terraform/ec2/main.tf

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
    cidr_blocks = [var.allowed_ip_range]
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
    cidr_blocks = [var.allowed_ip_range]
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

resource "aws_ebs_volume" "kafka_data" {
  availability_zone = var.availability_zone
  size             = 64
  type             = "gp3"
  iops             = 3000
  throughput       = 125

  tags = {
    Name = "kafka-data"
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

  user_data = <<-EOF
              #!/bin/bash
              # Update system
              sudo apt-get update
              sudo apt-get upgrade -y

              # Install Java
              sudo apt-get install -y openjdk-11-jdk

              # Create directories
              sudo mkdir -p /data/kafka
              sudo mkdir -p /data/airflow

              # Mount EBS volume
              sudo mkfs -t ext4 ${aws_ebs_volume.kafka_data.id}
              sudo mount ${aws_ebs_volume.kafka_data.id} /data/kafka

              # Add to fstab
              echo "${aws_ebs_volume.kafka_data.id} /data/kafka ext4 defaults,nofail 0 2" | sudo tee -a /etc/fstab

              # Set correct permissions
              sudo chown -R ubuntu:ubuntu /data
              EOF

  tags = {
    Name = "kafka-airflow-server"
  }
}

# terraform/ec2/variables.tf

variable "vpc_id" {
  description = "VPC ID where the instance will be created"
  type        = string
}

variable "subnet_id" {
  description = "Subnet ID where the instance will be created"
  type        = string
}

variable "availability_zone" {
  description = "Availability zone for the instance"
  type        = string
}

variable "key_pair_name" {
  description = "Name of the existing key pair"
  type        = string
}

variable "iam_role_name" {
  description = "Name of the existing IAM role"
  type        = string
}

variable "vpc_cidr" {
  description = "CIDR block of the VPC"
  type        = string
}

variable "allowed_ip_range" {
  description = "CIDR block for allowed IP range"
  type        = string
  default     = "0.0.0.0/0"  # Change this to your IP range for better security
}

# terraform/ec2/outputs.tf

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
  value = aws_security_group.kafka_airflow_sg.id
}