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
