# terraform/snowflake/warehouse/main.tf
resource "snowflake_warehouse" "loading_wh" {
  name           = "LOADING_WH_${upper(var.environment)}"
  warehouse_size = var.warehouse_size
  auto_suspend   = var.auto_suspend
  auto_resume    = var.auto_resume
  min_cluster_count = var.min_cluster_count
  max_cluster_count = var.max_cluster_count
}

resource "snowflake_warehouse" "analytics_wh" {
  name           = "ANALYTICS_WH_${upper(var.environment)}"
  warehouse_size = var.warehouse_size
  auto_suspend   = var.auto_resume
  auto_resume    = true
}


output "loading_warehouse_name" {
  description = "Name of the loading warehouse"
  value       = snowflake_warehouse.loading_wh.name
}

output "analytics_warehouse_name" {
  description = "Name of the analytics warehouse"
  value       = snowflake_warehouse.analytics_wh.name
}

output "loading_warehouse_size" {
  description = "Size of the loading warehouse"
  value       = snowflake_warehouse.loading_wh.warehouse_size
}
