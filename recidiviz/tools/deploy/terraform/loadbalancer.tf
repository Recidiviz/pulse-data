
# Setting up load balancer
# Drawn from https://github.com/terraform-google-modules/terraform-google-lb-http/blob/master/examples/cloudrun/main.tf

resource "google_compute_region_network_endpoint_group" "recidiviz_data_app_engine_neg" {
  provider              = google-beta
  name                  = "pulse-data-app-engine-neg"
  network_endpoint_type = "SERVERLESS"
  region                = var.region
  app_engine {
    service = "default"
  }
}

module "app-engine-load-balancer" {
  source  = "GoogleCloudPlatform/lb-http/google//modules/serverless_negs"
  version = "~> 11.0.0"
  name    = "app-engine-loadbalancer-${var.project_id}"
  project = var.project_id

  ssl                             = true
  ssl_policy                      = google_compute_ssl_policy.restricted-ssl-policy.name
  managed_ssl_certificate_domains = local.is_production ? ["ae-prod.recidiviz.org"] : ["ae-staging.recidiviz.org"]
  https_redirect                  = true

  backends = {
    default = {
      description = null
      groups = [
        {
          group = google_compute_region_network_endpoint_group.recidiviz_data_app_engine_neg.id
        }
      ]
      enable_cdn = true
      cdn_policy = {
        cache_mode                   = "CACHE_ALL_STATIC"
        default_ttl                  = 3600
        max_ttl                      = 86400
        client_ttl                   = 3600
        negative_caching             = true
        serve_while_stale            = 86400
        signed_url_cache_max_age_sec = 0
      }
      security_policy = google_compute_security_policy.recidiviz-waf-policy.id
      custom_request_headers = [
        "X-Client-Geo-Location: {client_region_subdivision}, {client_city}",
        "TLS_VERSION: {tls_version}",
        "TLS_CIPHER_SUITE: {tls_cipher_suite}",
        "CLIENT_ENCRYPTED: {client_encrypted}"
      ]
      custom_response_headers = null
      iap_config = {
        enable               = false
        oauth2_client_id     = ""
        oauth2_client_secret = ""
      }
      log_config = {
        enable      = true
        sample_rate = 1
      }
    }
  }
}
