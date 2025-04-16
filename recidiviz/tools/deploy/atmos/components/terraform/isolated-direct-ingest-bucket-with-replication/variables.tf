variable "project_id" {
  type        = string
  description = "Project we are provisioning to"
}

variable "region" {
  type        = string
  description = "Region we are provisioning to"
}

variable "state_code" {
  type        = string
  description = "State we are provisioning for (e.g. `US_NC`)"
}

variable "replication_spec_include_prefixes_file_path" {
  type        = string
  description = "The path, relative to this module, to a newline-separated list of file prefixes to *include* when replicating to the ingest bucket."
  default     = null
}

variable "replication_spec_exclude_prefixes_file_path" {
  type        = string
  description = "The path, relative to this module, to a newline-separated list of file prefixes to *exclude* when replicating to the ingest bucket."
  default     = null
}
