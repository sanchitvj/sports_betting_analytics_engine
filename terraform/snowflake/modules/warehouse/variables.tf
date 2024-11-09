# terraform/snowflake/warehouse/variables.tf
variable "environment" {
  description = "Deployment environment"
  type        = string
}

variable "warehouse_size" {
  description = "Size of the warehouse"
  type        = string
  default     = "XSMALL"
}

variable "auto_suspend" {
  description = "Auto suspend time in seconds"
  type        = number
  default     = 120
}

variable "auto_resume" {
  description = "Enable auto resume"
  type        = bool
  default     = true
}

variable "min_cluster_count" {
  description = "Minimum number of clusters"
  type        = number
  default     = 1
}

variable "max_cluster_count" {
  description = "Maximum number of clusters"
  type        = number
  default     = 3
}