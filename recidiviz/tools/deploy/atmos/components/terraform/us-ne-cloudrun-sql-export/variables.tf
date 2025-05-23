variable "project_id" {
  type        = string
  description = "Project we are provisioning to"
}

variable "region" {
  type        = string
  description = "Region we are provisioning to"
}

variable "ingest_bucket_name" {
  type        = string
  description = "The ingest bucket to upload data to"
}

variable "service_account_email" {
  type        = string
  description = "Email of the service account to use for execution"
}

variable "registry_project_id" {
  type        = string
  description = "The project ID of the registry to pull the Docker image from"
}

variable "vpc_network" {
  type        = string
  description = "The VPC network to use for the Cloud Run job"
}

variable "vpc_subnetwork" {
  type        = string
  description = "The VPC subnetwork to use for the Cloud Run job"
}

variable "run_schedule" {
  type        = string
  description = "The schedule for the Cloud Scheduler job in cron format in UTC"
}
