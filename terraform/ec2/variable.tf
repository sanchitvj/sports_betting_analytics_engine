variable "subnet_id" {
  description = "Subnet ID where the instance will be created"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID where security group will be created"
  type        = string
}

variable "key_pair_name" {
  description = "Name of the existing key pair"
  type        = string
}

variable "instance_type" {
  description = "Instance type"
  type = string
  default = "t2.medium"
}

variable "iam_role_name" {
  description = "Name of the existing IAM role"
  type        = string
}

variable "kafka_version" {
  description = "Kafka version to install"
  type        = string
  default     = "3.8.1"
}

variable "zookeeper_version" {
  description = "Zookeeper version to install"
  type        = string
  default     = "3.8.0"
}

variable "vpc_cidr" {
  description = "CIDR block of the VPC"
  type        = string
}

variable "allowed_cidr_blocks" {
  description = "List of CIDR blocks allowed to access Kafka"
  type        = string
  default     = "0.0.0.0/0"
}