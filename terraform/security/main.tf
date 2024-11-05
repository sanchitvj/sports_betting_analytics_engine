# terraform/security/main.tf
resource "aws_security_group" "kafka_sg" {
  name        = "kafka-security-group"
  description = "Security group for Kafka cluster"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr_blocks
  }

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr_blocks
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "kafka-security-group"
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