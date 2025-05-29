data "google_secret_manager_secret_version" "db_password" {
  secret  = "resource_search_db_password"
  project = var.project_id
}

data "google_secret_manager_secret_version" "db_user" {
  secret  = "resource_search_db_user"
  project = var.project_id
}

module "postgresql" {
  name                        = var.sql_instance_name
  source                      = "../../vendor/cloud-sql-instance/modules/postgresql"
  project_id                  = var.project_id
  region                      = var.location
  zone                        = "us-central1-a"
  database_version            = "POSTGRES_16"
  tier                        = "db-custom-1-3840"

  enable_default_user = true
  user_name           = data.google_secret_manager_secret_version.db_user.secret_data
  user_password       = data.google_secret_manager_secret_version.db_password.secret_data
 
  backup_configuration = {
    enabled                        = var.is_backup_enabled
    start_time                     = "04:00" # UTC
    location                       = var.location
    point_in_time_recovery_enabled = true
    retained_backups               = 7
    retention_unit                 = "COUNT" 
  
  }
  
  insights_config = {
    query_string_length     = 1024
    record_application_tags = false
    record_client_address   = false
  }
}
