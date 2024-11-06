variable "subnet_id" {
  description = "Subnet ID where the instance will be created"
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

variable "kafka_version" {
  description = "Kafka version to install"
  type        = string
  default     = "3.4.0"
}

variable "zookeeper_version" {
  description = "Zookeeper version to install"
  type        = string
  default     = "3.8.0"
}

variable "vpc_id" {
  description = "VPC ID where security group will be created"
  type        = string
}

variable "vpc_cidr" {
  description = "CIDR block of the VPC"
  type        = string
}

variable "allowed_cidr_blocks" {
  description = "List of CIDR blocks allowed to access Kafka"
  type        = list(string)
  default     = ["0.0.0.0/0"]
}