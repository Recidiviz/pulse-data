variable "project_id" {
  type        = string
  description = "Project we are provisioning to"
}

variable "state_code" {
  type        = string
  description = "State we are provisioning for (e.g. `US_NC`)"
}

variable "ingest_bucket_name" {
  type        = string
  description = "Bucket name for the GCS bucket that receives raw state data"
}
