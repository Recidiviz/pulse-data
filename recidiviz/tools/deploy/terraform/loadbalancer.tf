
# Setting up load balancer
# Drawn from https://github.com/terraform-google-modules/terraform-google-lb-http/blob/master/examples/cloudrun/main.tf
resource "google_compute_region_network_endpoint_group" "case_triage_neg" {
  provider              = google-beta
  name                  = "case-triage-neg"
  network_endpoint_type = "SERVERLESS"
  region                = var.region
  cloud_run {
    service = google_cloud_run_service.case-triage.name
  }
}

resource "google_compute_region_network_endpoint_group" "recidiviz_data_app_engine_neg" {
  provider              = google-beta
  name                  = "pulse-data-app-engine-neg"
  network_endpoint_type = "SERVERLESS"
  region                = var.region
  app_engine {
    service = "default"
  }
}

resource "google_compute_url_map" "urlmap" {
  name            = "unified-product-url-map"
  default_service = google_compute_region_network_endpoint_group.case_triage_neg.id
  # Case Triage routing rules
  host_rule {
    hosts        = local.is_production ? ["app-prod.recidiviz.org", "app.recidiviz.org"] : ["app-staging.recidiviz.org"]
    description  = "Case Triage traffic (app-ENV.recidiviz.org and app.recidiviz.org) is routed to the Case Triage service on Cloud Run"
    path_matcher = "case-triage"
  }
  path_matcher {
    name            = "case-triage"
    default_service = google_compute_region_network_endpoint_group.case_triage_neg.id
  }
  # AE routing rules
  host_rule {
    hosts        = local.is_production ? ["ae-prod.recidiviz.org"] : ["ae-staging.recidiviz.org"]
    description  = "The pulse-data backend traffic (ae-ENV.recidiviz.org) is routed to the backend service running on AE"
    path_matcher = "default-app-engine"
  }
  path_matcher {
    name            = "default-app-engine"
    default_service = google_compute_region_network_endpoint_group.recidiviz_data_app_engine_neg.id
  }
}

module "unified-product-load-balancer-v2" {
  source  = "GoogleCloudPlatform/lb-http/google//modules/serverless_negs"
  version = "~> 6.2.0"
  name    = "unified-product-lb-${var.project_id}"
  project = var.project_id

  ssl                             = true
  ssl_policy                      = local.is_production ? google_compute_ssl_policy.modern-ssl-policy.name : google_compute_ssl_policy.restricted-ssl-policy.name
  url_map                         = google_compute_url_map.urlmap.self_link
  create_url_map                  = false
  managed_ssl_certificate_domains = local.is_production ? ["app-prod.recidiviz.org", "app.recidiviz.org", "ae-prod.recidiviz.org"] : ["app-staging.recidiviz.org", "ae-staging.recidiviz.org"]
  https_redirect                  = true

  backends = {
    case_triage = {
      description = null
      groups = [
        {
          group = google_compute_region_network_endpoint_group.case_triage_neg.id
        }
      ]
      enable_cdn      = true
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
        sample_rate = null
      }
    }
    ae = {
      description = null
      groups = [
        {
          group = google_compute_region_network_endpoint_group.recidiviz_data_app_engine_neg.id
        }
      ]
      enable_cdn      = true
      security_policy = google_compute_security_policy.recidiviz-waf-policy-non-enforcement.id
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
        sample_rate = null
      }
    }
  }
}