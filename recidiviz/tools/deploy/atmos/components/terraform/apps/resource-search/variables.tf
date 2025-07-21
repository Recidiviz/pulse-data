variable "sql_instance_name" {
  type        = string
  description = "The name of the SQL instance"
  default = null
}

variable "location" {
  type        = string
  description = "The GCP location (us-east1, us-central1, etc) that we are deploying the service to"
  default     = null
}

variable "project_id" {
  type        = string
  description = "ID of the Google Cloud Project"
  default     = null
}

variable "is_backup_enabled"{
  type        = bool
  description = "A boolean flag describing in backups are configured for the DB"
  default     = null
}

variable "big_query_instance_name" {
  description = "The ID for the BigQuery dataset. Should be unique per environment (e.g., 'resource_search_staging' or 'resource_search_prod')."
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
