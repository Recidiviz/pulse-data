output "cluster" {
  description = "Cluster ID"
  value = module.runner-cluster.name
}

output "region" {
  description = "Region deployed to"
  value = module.runner-cluster.region
}

output "zones" {
  value = var.zones
  description = "Zones deployed to"
}

output "kubernetes_service_account" {
  value = module.runner-cluster.service_account
  sensitive = true
  description = "K8s service account"
}

output "kubernetes_endpoint" {
  value = module.runner-cluster.endpoint
  sensitive = true
  description = "Endpoint to use when connecting to the K8s cluster"
}

output "kubernetes_ca_certificate" {
  value = module.runner-cluster.ca_certificate
  sensitive = true
  description = "CA Certificate for connecting to the K8s cluster"
}
