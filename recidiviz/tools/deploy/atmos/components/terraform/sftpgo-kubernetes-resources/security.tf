# Regional Cloud Armor Security Policy for HTTPS Admin Interface
# Only created if sftpgo_admin_allowed_ips is non-empty
resource "google_compute_region_security_policy" "admin_allowlist" {
  count = length(local.sftpgo_admin_allowed_ips) > 0 ? 1 : 0

  name        = "sftpgo-admin-allowlist"
  region      = var.region
  description = "IP allowlist for SFTPGo HTTPS admin interface"
  project     = var.project_id
  type        = "CLOUD_ARMOR"
}

# Allow rule for specified IPs
resource "google_compute_region_security_policy_rule" "admin_allow" {
  count = length(local.sftpgo_admin_allowed_ips) > 0 ? 1 : 0

  project         = var.project_id
  region          = var.region
  security_policy = google_compute_region_security_policy.admin_allowlist[0].name
  description     = "Allow access from specified IP ranges"
  priority        = 1000
  action          = "allow"

  match {
    versioned_expr = "SRC_IPS_V1"
    config {
      src_ip_ranges = local.sftpgo_admin_allowed_ips
    }
  }

  depends_on = [google_compute_region_security_policy.admin_allowlist]
}

# Deny all other traffic
resource "google_compute_region_security_policy_rule" "admin_deny_all" {
  count = length(local.sftpgo_admin_allowed_ips) > 0 ? 1 : 0

  project         = var.project_id
  region          = var.region
  security_policy = google_compute_region_security_policy.admin_allowlist[0].name
  description     = "Deny all other traffic"
  priority        = 2147483647 # Maximum priority (lowest precedence)
  action          = "deny(403)"

  match {
    versioned_expr = "SRC_IPS_V1"
    config {
      src_ip_ranges = ["*"]
    }
  }

  depends_on = [google_compute_region_security_policy.admin_allowlist]
}

# Firewall Rule for SFTP IP Allowlisting
# Only created if sftp_allowed_ips is non-empty
# This rule allows SFTP traffic (port 22) only from specified IPs
resource "google_compute_firewall" "sftp_allowlist" {
  count = length(local.sftp_allowed_ips) > 0 ? 1 : 0

  name    = "fw-allow-sftp-allowlist"
  network = data.google_compute_network.gke_network.self_link
  project = data.google_container_cluster.primary.project

  direction = "INGRESS"
  priority  = 900 # Higher priority (lower number) than default rules

  # Source IPs - Only specified IPs can access SFTP
  source_ranges = local.sftp_allowed_ips

  # Target tags - GKE nodes
  target_tags = [
    for pool in data.google_container_cluster.primary.node_pool :
    "gke-${data.google_container_cluster.primary.name}-${pool.name}"
  ]

  # Allow SFTP traffic
  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  description = "Allow SFTP access only from specified IP ranges"

  log_config {
    metadata = "INCLUDE_ALL_METADATA"
  }
}

# Firewall Rule to DENY all other SFTP traffic if allowlist is enabled
# This ensures that if the allowlist is configured, ONLY those IPs can access SFTP
resource "google_compute_firewall" "sftp_deny_all" {
  count = length(local.sftp_allowed_ips) > 0 ? 1 : 0

  name    = "fw-deny-sftp-default"
  network = data.google_compute_network.gke_network.self_link
  project = data.google_container_cluster.primary.project

  direction = "INGRESS"
  priority  = 1100 # Lower priority (higher number) than allowlist rule

  # Source: All IPs
  source_ranges = ["0.0.0.0/0"]

  # Target tags - GKE nodes
  target_tags = [
    for pool in data.google_container_cluster.primary.node_pool :
    "gke-${data.google_container_cluster.primary.name}-${pool.name}"
  ]

  # Deny SFTP traffic
  deny {
    protocol = "tcp"
    ports    = ["22"]
  }

  description = "Deny SFTP access from all IPs not in allowlist"

  log_config {
    metadata = "INCLUDE_ALL_METADATA"
  }
}
