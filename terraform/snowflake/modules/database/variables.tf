# terraform/snowflake/database/variables.tf
variable "environment" {
  description = "Deployment environment"
  type        = string
}

variable "data_retention_days" {
  description = "Number of days to retain data"
  type        = number
  default     = 1
}

variable "database_name" {
  description = "Name of the database"
  type        = string
  default     = "SPORTS_BETTING"
}