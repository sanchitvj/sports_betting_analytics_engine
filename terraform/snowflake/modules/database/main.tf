# terraform/snowflake/modules/database/main.tf
resource "snowflake_database" "sports_betting" {
  name                        = "SPORTS_BETTING_${upper(var.environment)}"
  data_retention_time_in_days = 1
}

resource "snowflake_schema" "raw" {
  database = snowflake_database.sports_betting.name
  name     = "RAW"
}

resource "snowflake_schema" "staging" {
  database = snowflake_database.sports_betting.name
  name     = "STAGING"
}

resource "snowflake_schema" "analytics" {
  database = snowflake_database.sports_betting.name
  name     = "ANALYTICS"
}


output "database_name" {
  description = "Name of the created database"
  value       = snowflake_database.sports_betting.name
}

output "raw_schema_name" {
  description = "Name of the RAW schema"
  value       = snowflake_schema.raw.name
}

output "staging_schema_name" {
  description = "Name of the STAGING schema"
  value       = snowflake_schema.staging.name
}

output "analytics_schema_name" {
  description = "Name of the ANALYTICS schema"
  value       = snowflake_schema.analytics.name
}