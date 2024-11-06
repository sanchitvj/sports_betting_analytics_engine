provider "aws" {
  region = var.aws_region
}

resource "aws_vpc" "kafka_vpc" {
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = "kafka-vpc"
    Environment = var.environment
  }
}


output "vpc_id" {
  value = aws_vpc.kafka_vpc.id
}


terraform {
  backend "s3" {
    bucket         = ""
    key            = "state_file/vpc.tfstate" # modify
    region         = ""
    encrypt        = true
    dynamodb_table = ""
  }
}