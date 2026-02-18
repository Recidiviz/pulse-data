variable "project_id" {
  description = "The GCP project to deploy the load balancer and WAF into"
  type        = string
}

variable "managed_ssl_certificate_domains" {
  description = "Domains that should have Google-managed SSL certs in the load balancer"
  type        = list(string)
}
