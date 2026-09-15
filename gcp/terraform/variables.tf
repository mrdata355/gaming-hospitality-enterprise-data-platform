variable "project_id" { type = string }
variable "region" { type = string default = "us-west1" }
variable "environment" { type = string default = "dev" }
variable "cloud_run_image" { type = string }
variable "billing_account" { type = string default = "" sensitive = true }
variable "monthly_budget_usd" { type = number default = 100 }
variable "enable_budget" { type = bool default = false }
