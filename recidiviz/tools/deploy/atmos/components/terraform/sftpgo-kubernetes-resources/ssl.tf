# Create Cloud DNS zone for subdomain delegation
resource "google_dns_managed_zone" "sftpgo_admin" {
  name        = "sftpgo-admin-zone"
  dns_name    = "${local.sftpgo_admin_domain}."
  description = "DNS zone for SFTPGO admin interface - delegated from Squarespace"
  project     = var.project_id
}

# Create A record pointing to the load balancer IP
resource "google_dns_record_set" "sftpgo_admin_a" {
  managed_zone = google_dns_managed_zone.sftpgo_admin.name
  name         = "${local.sftpgo_admin_domain}."
  type         = "A"
  ttl          = 300
  rrdatas      = [google_compute_address.sftpgo_regional.address]
  project      = var.project_id
}

resource "google_project_service" "certificate_manager" {
  project = var.project_id
  service = "certificatemanager.googleapis.com"

  disable_on_destroy = false
}

# DNS authorization proves domain ownership to Certificate Manager. Google uses the
# CNAME record below to issue and renew the certificate without any manual applies.
resource "google_certificate_manager_dns_authorization" "sftpgo_admin" {
  name     = "sftpgo-admin-dns-auth"
  location = var.region
  domain   = local.sftpgo_admin_domain
  type     = "PER_PROJECT_RECORD"
  project  = var.project_id

  depends_on = [google_project_service.certificate_manager]
}

resource "google_dns_record_set" "sftpgo_admin_dns_auth" {
  managed_zone = google_dns_managed_zone.sftpgo_admin.name
  name         = google_certificate_manager_dns_authorization.sftpgo_admin.dns_resource_record[0].name
  type         = google_certificate_manager_dns_authorization.sftpgo_admin.dns_resource_record[0].type
  ttl          = 300
  rrdatas      = [google_certificate_manager_dns_authorization.sftpgo_admin.dns_resource_record[0].data]
  project      = var.project_id
}

# Google-managed certificate, renewed automatically by Certificate Manager
resource "google_certificate_manager_certificate" "sftpgo_admin" {
  name     = "sftpgo-admin-cert"
  location = var.region
  project  = var.project_id

  managed {
    domains            = [local.sftpgo_admin_domain]
    dns_authorizations = [google_certificate_manager_dns_authorization.sftpgo_admin.id]
  }

  depends_on = [google_dns_record_set.sftpgo_admin_dns_auth]
}

# Wait for the certificate to be issued before attaching it to the HTTPS proxy, so
# the load balancer never serves a certificate that is still provisioning
resource "null_resource" "wait_for_managed_cert" {
  provisioner "local-exec" {
    command = <<-EOT
      echo "Waiting for Certificate Manager to issue ${google_certificate_manager_certificate.sftpgo_admin.name}..."
      for i in $(seq 1 60); do
        state=$(gcloud certificate-manager certificates describe ${google_certificate_manager_certificate.sftpgo_admin.name} \
          --location=${var.region} --project=${var.project_id} --format='value(managed.state)')
        if [ "$state" = "ACTIVE" ]; then
          echo "Certificate is ACTIVE!"
          exit 0
        fi
        echo "Attempt $i/60: Certificate state is [$state], waiting 30 seconds..."
        sleep 30
      done
      echo "ERROR: Certificate not ACTIVE within 30 minutes. Check provisioning issues with:"
      echo "  gcloud certificate-manager certificates describe ${google_certificate_manager_certificate.sftpgo_admin.name} --location=${var.region} --project=${var.project_id}"
      echo "then rerun the apply."
      exit 1
    EOT
  }

  triggers = {
    certificate_id = google_certificate_manager_certificate.sftpgo_admin.id
  }
}
