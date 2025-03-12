variable "project_id" {
  type = string
  description = "Project we are provisioning to"
}

variable "region" {
  type = string
  description = "Region we are provisioning to"
}

variable "kubernetes_endpoint" {
  type = string
  sensitive = true
  description = "Endpoint to use when connecting to the K8s cluster"
}

variable "kubernetes_ca_certificate" {
  type = string
  sensitive = true
  description = "CA Certificate for connecting to the K8s cluster"
}
