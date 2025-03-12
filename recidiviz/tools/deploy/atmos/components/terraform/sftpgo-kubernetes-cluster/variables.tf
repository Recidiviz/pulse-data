variable "project_id" {
  description = "Project to deploy the runner cluster to"
  type = string
}

variable "region" {
  description = "Region to deploy to"
  type = string
  default = "us-central1"
}

variable "zones" {
  description = "Zones to run in"
  type = set(string)
  default = ["us-central1-b"]
}
