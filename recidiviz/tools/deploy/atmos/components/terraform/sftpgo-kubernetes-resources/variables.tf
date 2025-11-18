variable "project_id" {
  type        = string
  description = "Project we are provisioning to"
}

variable "region" {
  type        = string
  description = "Region we are provisioning to"
}

variable "zone" {
  type        = string
  description = "Zone we are provisioning to"
}

variable "state_code" {
  type        = string
  description = "State we are provisioning for"
}

variable "sftp_bucket_name" {
  type        = string
  description = "GCS bucket that is used to back the SFTPGo server"
}

variable "kubernetes_endpoint" {
  type        = string
  sensitive   = true
  description = "Endpoint to use when connecting to the K8s cluster"
}

variable "kubernetes_ca_certificate" {
  type        = string
  sensitive   = true
  description = "CA Certificate for connecting to the K8s cluster"
}

variable "config_file" {
  type        = string
  description = "Path to SOPS-encrypted YAML file containing secrets and configuration (e.g., secrets.ingest-project.enc.yaml)"
}
