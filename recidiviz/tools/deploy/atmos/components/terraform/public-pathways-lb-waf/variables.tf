variable "project_id" {
  description = "The GCP project to deploy the load balancer and WAF into"
  type        = string
}

variable "managed_ssl_certificate_domains" {
  description = "Domains that should have Google-managed SSL certs in the load balancer"
  type        = list(string)
}

variable "min_tls_version" {
  description = "Minimum TLS version for the HTTPS frontend (e.g. TLS_1_2). When set, a RESTRICTED-profile SSL policy is created and attached to the load balancer; when null, the frontend uses GCP's default SSL policy, which accepts TLS 1.0."
  type        = string
  default     = null
}
