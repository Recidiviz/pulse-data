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

variable "preserve_source_time_created_as_custom_time" {
  type        = bool
  description = "When true, the replication job preserves each source object's creation time as the destination object's `customTime`. The ingest filename normalization Cloud Function reads that `customTime` as the file's update_datetime. Enable for states whose files should be dated by when they were delivered to the SFTP bucket rather than when they were normalized."
  default     = false
}
