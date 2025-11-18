locals {
  load_balancing_ips = tolist([
    "130.211.0.0/22", # Google Cloud Load Balancing
    "35.191.0.0/16",  # Google Cloud Health Checks
  ])
}

# Wait for GKE to create the NEG after the service is created
resource "null_resource" "wait_for_neg" {
  depends_on = [kubernetes_service_v1.sftpgo_admin_http]

  provisioner "local-exec" {
    command = <<-EOT
      echo "Waiting for GKE to create NEG sftpgo-admin-neg-l7..."
      for i in {1..30}; do
        if gcloud compute network-endpoint-groups describe sftpgo-admin-neg-l7 \
           --zone=${var.zone} --project=${var.project_id} 2>/dev/null; then
          echo "NEG created successfully!"
          exit 0
        fi
        echo "Attempt $i/30: NEG not found yet, waiting 10 seconds..."
        sleep 10
      done
      echo "ERROR: Timeout waiting for NEG to be created by GKE"
      exit 1
    EOT
  }

  triggers = {
    # Re-run if service changes
    service_uid = kubernetes_service_v1.sftpgo_admin_http.metadata[0].uid
  }
}

# Reference the GKE-managed NEG
data "google_compute_network_endpoint_group" "sftpgo" {
  name = "sftpgo-admin-neg-l7"
  zone = var.zone

  depends_on = [null_resource.wait_for_neg]
}

resource "google_compute_region_health_check" "sftpgo" {
  name   = "sftpgo-health-check"
  region = var.region

  http_health_check {
    port         = 8080
    request_path = "/healthz" # Or whatever your health endpoint is
  }

  timeout_sec         = 5
  check_interval_sec  = 10
  healthy_threshold   = 2
  unhealthy_threshold = 3
}


resource "google_compute_region_backend_service" "sftpgo" {
  name                  = "sftpgo-backend-regional"
  region                = var.region
  protocol              = "HTTP"
  port_name             = "http"
  timeout_sec           = 30
  health_checks         = [google_compute_region_health_check.sftpgo.id]
  load_balancing_scheme = "EXTERNAL_MANAGED"

  # Attach Cloud Armor security policy if IP allowlist is configured
  security_policy = length(local.sftpgo_admin_allowed_ips) > 0 ? google_compute_region_security_policy.admin_allowlist[0].self_link : null

  # Enable Identity-Aware Proxy if configured
  # OAuth credentials must be created manually in GCP Console and stored in SOPS
  dynamic "iap" {
    for_each = local.iap_enabled ? [1] : []
    content {
      oauth2_client_id     = local.iap_oauth_client_id
      oauth2_client_secret = local.iap_oauth_client_secret
      enabled              = local.iap_enabled
    }
  }

  backend {
    group           = data.google_compute_network_endpoint_group.sftpgo.id
    balancing_mode  = "RATE"
    max_rate        = 100
    capacity_scaler = 1.0
  }

  # Ensure security policy and rules are created before backend service
  depends_on = [
    google_compute_region_security_policy.admin_allowlist,
    google_compute_region_security_policy_rule.admin_allow,
    google_compute_region_security_policy_rule.admin_deny_all
  ]
}


resource "google_compute_region_url_map" "sftpgo_admin" {
  name            = "sftpgo-url-map-regional"
  region          = var.region
  default_service = google_compute_region_backend_service.sftpgo.id

  host_rule {
    hosts        = [local.sftpgo_admin_domain]
    path_matcher = "allpaths"
  }

  path_matcher {
    name            = "allpaths"
    default_service = google_compute_region_backend_service.sftpgo.id
  }
}

# SSL Policy to enforce TLS 1.2+ only (disable TLS 1.0 and 1.1)
resource "google_compute_region_ssl_policy" "tls12_modern" {
  name            = "sftpgo-tls12-policy"
  region          = var.region
  profile         = "MODERN"
  min_tls_version = "TLS_1_2"

  description = "SSL policy enforcing TLS 1.2+ for SFTPGo admin interface"
}

resource "google_compute_region_target_https_proxy" "sftpgo" {
  name             = "sftpgo-https-proxy-regional"
  region           = var.region
  url_map          = google_compute_region_url_map.sftpgo_admin.id
  ssl_certificates = [google_compute_region_ssl_certificate.sftpgo.id]
  ssl_policy       = google_compute_region_ssl_policy.tls12_modern.id
}

resource "google_compute_forwarding_rule" "sftpgo_https" {
  name                  = "sftpgo-forwarding-rule-https"
  region                = var.region
  ip_protocol           = "TCP"
  load_balancing_scheme = "EXTERNAL_MANAGED"
  port_range            = "443"
  target                = google_compute_region_target_https_proxy.sftpgo.id
  network_tier          = "PREMIUM"
  ip_address            = google_compute_address.sftpgo_regional.id
  network               = data.google_compute_network.gke_network.id

  depends_on = [google_compute_subnetwork.proxy_only]
}

# HTTP to HTTPS redirect
resource "google_compute_region_url_map" "http_redirect" {
  name   = "sftpgo-http-redirect-regional"
  region = var.region

  default_url_redirect {
    https_redirect         = true
    redirect_response_code = "MOVED_PERMANENTLY_DEFAULT"
    strip_query            = false
  }
}

resource "google_compute_region_target_http_proxy" "http_redirect" {
  name    = "sftpgo-http-proxy-regional"
  region  = var.region
  url_map = google_compute_region_url_map.http_redirect.id
}

resource "google_compute_address" "sftpgo_regional" {
  name   = "sftpgo-admin-regional-ip"
  region = var.region
}


resource "google_compute_forwarding_rule" "sftpgo_http" {
  name                  = "sftpgo-forwarding-rule-http"
  region                = var.region
  ip_protocol           = "TCP"
  load_balancing_scheme = "EXTERNAL_MANAGED"
  port_range            = "80"
  target                = google_compute_region_target_http_proxy.http_redirect.id
  network_tier          = "PREMIUM"
  ip_address            = google_compute_address.sftpgo_regional.id
  network               = data.google_compute_network.gke_network.id

  depends_on = [google_compute_subnetwork.proxy_only]
}

resource "google_compute_subnetwork" "proxy_only" {
  name          = "sftpgo-l7-ilb-subnet"
  ip_cidr_range = "10.1.10.0/24"
  region        = var.region
  network       = data.google_compute_network.gke_network.id
  purpose       = "REGIONAL_MANAGED_PROXY" # This makes it a proxy-only subnet
  role          = "ACTIVE"
}

# Allow health checks to reach the VM IPs
# Get the network from the cluster
data "google_compute_network" "gke_network" {
  name    = regex("projects/[^/]+/global/networks/([^/]+)", data.google_container_cluster.primary.network)[0]
  project = data.google_container_cluster.primary.project
}

# Create the firewall rule
resource "google_compute_firewall" "allow_health_check_and_proxy" {
  name    = "fw-allow-health-check-and-proxy"
  network = data.google_compute_network.gke_network.self_link
  project = data.google_container_cluster.primary.project

  # Allow ingress
  direction = "INGRESS"

  # Priority (optional, defaults to 1000)
  priority = 1000

  # Source ranges - Google health check and proxy IPs
  source_ranges = concat(
    local.load_balancing_ips,
    [google_compute_subnetwork.proxy_only.ip_cidr_range]
  )

  # Target tags - extract from node pools
  # GKE node tags follow pattern: gke-{cluster-name}-{node-pool-name}
  target_tags = [
    for pool in data.google_container_cluster.primary.node_pool :
    "gke-${data.google_container_cluster.primary.name}-${pool.name}"
  ]

  # Rules
  allow {
    protocol = "tcp"
    ports    = ["8080"]
  }

  # Optional: Add logging
  log_config {
    metadata = "INCLUDE_ALL_METADATA"
  }
}
