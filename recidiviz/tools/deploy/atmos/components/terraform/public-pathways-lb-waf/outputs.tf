output "external_ip" {
  description = "The external IPv4 assigned to the load balancer"
  value       = module.load_balancer.external_ip
}

output "waf_policy_id" {
  description = "The ID of the WAF security policy"
  value       = google_compute_security_policy.public-pathways-waf-policy.id
}

output "backend_services" {
  description = "The backend service resources"
  value       = module.load_balancer.backend_services
  sensitive   = true
}
