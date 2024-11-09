variable "environment" {
  description = "Deployment environment"
  type        = string
}

variable "admin_users" {
  description = "List of users to be assigned admin role"
  type        = list(string)
  default     = []
}

variable "analyst_users" {
  description = "List of users to be assigned analyst role"
  type        = list(string)
  default     = []
}