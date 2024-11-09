# terraform/snowflake/main.tf
terraform {
  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.87"
    }
  }
  backend "s3" {
    bucket = "your-terraform-state-bucket"
    key    = "snowflake/terraform.tfstate"
    region = "us-east-1"
  }
}

provider "snowflake" {
  account    = var.snowflake_account
  username   = var.snowflake_user
  private_key = var.snowflake_private_key
}

module "warehouse" {
  source = "./modules/warehouse"
  environment = var.environment
}

module "database" {
  source = "./modules/database"
  environment = var.environment
}

module "roles" {
  source = "./modules/roles"
  environment = var.environment
}