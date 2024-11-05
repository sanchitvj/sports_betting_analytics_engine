variable "broker_count" {
  description = "Number of Kafka brokers"
  type        = number
  default     = 3
}

variable "kafka_ami" {
  description = "AMI ID for Kafka instances"
  type        = string
}

variable "instance_type" {
  description = "Instance type for Kafka brokers"
  type        = string
  default     = "t3.medium"
}