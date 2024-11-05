variable "vpc_id" {
  description = "VPC ID"
  type        = string
}

variable "allowed_cidr_blocks" {
  description = "List of CIDR blocks allowed to access Kafka"
  type        = list(string)
  default     = ["0.0.0.0/0"]
}