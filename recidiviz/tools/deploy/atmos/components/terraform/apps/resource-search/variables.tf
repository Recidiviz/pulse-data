variable "sql_instance_name" {
  type        = string
  description = "The name of the SQL instance"
}

variable "location" {
  type        = string
  description = "The GCP location (us-east1, us-central1, etc) that we are deploying the service to"
}

variable "project_id" {
  type        = string
  description = "ID of the Google Cloud Project"
}

variable "is_backup_enabled"{
  type        = bool
  description = "A boolean flag describing in backups are configured for the DB"
  default     = false
}

variable "big_query_instance_name" {
  description = "The name for the BigQuery dataset. Should be unique per environment (e.g., 'resource_search_staging' or 'resource_search_prod')."
  type        = string
}

variable "big_query_instance_friendly_name" {
  description = "A user-friendly name for the BigQuery dataset, visible in the UI (e.g., 'Resource Search Data (Staging)')."
  type        = string
}

variable "big_query_instance_description" {
  description = "A detailed description of the BigQuery dataset's purpose and contents."
  type        = string
}

variable "postgresql" {
  type = object({
    databases: set(string)
  })
}

variable "tables" {
  type        = list(string)
  description = "A list of table names to transfer from PostgreSQL to BigQuery."
}

variable "service_account_id" {
  type        = string
  description = "The ID for the service account used by the app and for data transfer."
}
