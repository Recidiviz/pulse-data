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
