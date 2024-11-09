resource "snowflake_role" "loader_role" {
  name = "LOADER_ROLE_${upper(var.environment)}"
}

resource "snowflake_role" "analyst_role" {
  name = "ANALYST_ROLE_${upper(var.environment)}"
}

resource "snowflake_role_grants" "loader_grants" {
  role_name = snowflake_role.loader_role.name
  roles     = ["SYSADMIN"]
}

resource "snowflake_role_grants" "analyst_grants" {
  role_name = snowflake_role.analyst_role.name
  roles     = ["SYSADMIN"]
}


output "loader_role_name" {
  description = "Name of the loader role"
  value       = snowflake_role.loader_role.name
}

output "analyst_role_name" {
  description = "Name of the analyst role"
  value       = snowflake_role.analyst_role.name
}

output "role_assignments" {
  description = "Map of role assignments"
  value = {
    loader_role  = snowflake_role.loader_role.name
    analyst_role = snowflake_role.analyst_role.name
  }
}