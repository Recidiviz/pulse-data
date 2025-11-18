# Install cert-manager first (if not already installed)
# helm repo add jetstack https://charts.jetstack.io --force-update
resource "helm_release" "cert_manager" {
  name             = "cert-manager"
  repository       = "https://charts.jetstack.io"
  chart            = "cert-manager"
  namespace        = "cert-manager"
  create_namespace = true
  version          = "v1.13.0"

  set = [{
    name  = "installCRDs"
    value = "true"
  }]

  # Force Helm to update repo
  force_update = true
  wait         = true
  wait_for_jobs = true
  upgrade_install = true
}

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

# Service account for cert-manager to manage DNS records
resource "google_service_account" "cert_manager_dns" {
  account_id   = "cert-manager-dns01"
  display_name = "cert-manager DNS-01 solver"
  project      = var.project_id
}

# Grant DNS admin role to cert-manager service account
resource "google_project_iam_member" "cert_manager_dns_admin" {
  project = var.project_id
  role    = "roles/dns.admin"
  member  = "serviceAccount:${google_service_account.cert_manager_dns.email}"
}

# Bind Kubernetes service account to GCP service account via Workload Identity
resource "google_service_account_iam_member" "cert_manager_workload_identity" {
  service_account_id = google_service_account.cert_manager_dns.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[cert-manager/cert-manager]"
}

# Annotate the cert-manager Kubernetes service account for Workload Identity
resource "null_resource" "annotate_cert_manager_sa" {
  depends_on = [
    helm_release.cert_manager,
    google_service_account_iam_member.cert_manager_workload_identity
  ]

  provisioner "local-exec" {
    command = <<-EOT
      kubectl annotate serviceaccount cert-manager \
        -n cert-manager \
        iam.gke.io/gcp-service-account=${google_service_account.cert_manager_dns.email} \
        --overwrite
    EOT
  }

  triggers = {
    gcp_sa_email = google_service_account.cert_manager_dns.email
  }
}

# Create Let's Encrypt ClusterIssuer with Cloud DNS DNS-01 solver
resource "kubernetes_manifest" "letsencrypt_issuer" {
  manifest = {
    apiVersion = "cert-manager.io/v1"
    kind       = "ClusterIssuer"
    metadata = {
      name = "letsencrypt-prod"
    }
    spec = {
      acme = {
        server = "https://acme-v02.api.letsencrypt.org/directory"
        email  = "security@recidiviz.org"
        privateKeySecretRef = {
          name = "letsencrypt-prod-key"
        }
        solvers = [{
          selector = {
            dnsNames = [local.sftpgo_admin_domain]
          }
          dns01 = {
            cloudDNS = {
              project = var.project_id
              # No serviceAccountSecretRef needed - uses Workload Identity
            }
          }
        }]
      }
    }
  }

  depends_on = [
    helm_release.cert_manager,
    null_resource.annotate_cert_manager_sa
  ]
}

# Create Certificate resource
resource "kubernetes_manifest" "sftpgo_certificate" {
  manifest = {
    apiVersion = "cert-manager.io/v1"
    kind       = "Certificate"
    metadata = {
      name      = "sftpgo-admin-cert"
      namespace = "sftpgo"
    }
    spec = {
      secretName = "sftpgo-admin-tls"
      issuerRef = {
        name = "letsencrypt-prod"
        kind = "ClusterIssuer"
      }
      dnsNames = [local.sftpgo_admin_domain]
    }
  }

  depends_on = [kubernetes_manifest.letsencrypt_issuer]
}

# Wait for cert-manager to obtain the certificate
resource "null_resource" "wait_for_cert" {
  depends_on = [kubernetes_manifest.sftpgo_certificate]

  provisioner "local-exec" {
    command = <<-EOT
      echo "Waiting for cert-manager to obtain certificate..."
      for i in {1..60}; do
        if kubectl get secret sftpgo-admin-tls -n sftpgo 2>/dev/null | grep -q sftpgo-admin-tls; then
          # Check if the secret has actual certificate data
          if kubectl get secret sftpgo-admin-tls -n sftpgo -o jsonpath='{.data.tls\.crt}' 2>/dev/null | base64 -d | grep -q "BEGIN CERTIFICATE"; then
            echo "Certificate successfully obtained!"
            exit 0
          fi
        fi
        echo "Attempt $i/60: Certificate not ready yet, waiting 30 seconds..."
        sleep 30
      done
      echo "WARNING: Certificate not obtained within 30 minutes. You may need to check cert-manager logs."
      echo "Continue anyway - you can apply again once the certificate is ready."
      exit 0
    EOT
  }

  triggers = {
    # Re-run if certificate resource changes
    cert_checksum = sha256(jsonencode(kubernetes_manifest.sftpgo_certificate.manifest))
  }
}

# Read the certificate secret after it's been created
data "kubernetes_secret" "sftpgo_cert_actual" {
  metadata {
    name      = "sftpgo-admin-tls"
    namespace = "sftpgo"
  }

  depends_on = [null_resource.wait_for_cert]
}

# Create the regional SSL certificate in GCP using cert-manager's certificate
resource "google_compute_region_ssl_certificate" "sftpgo" {
  name        = "sftpgo-admin-cert-regional-${formatdate("YYYYMMDDhhmmss", timestamp())}"
  region      = var.region
  private_key = data.kubernetes_secret.sftpgo_cert_actual.data["tls.key"]
  certificate = data.kubernetes_secret.sftpgo_cert_actual.data["tls.crt"]

  lifecycle {
    create_before_destroy = true
    ignore_changes       = [name]  # Ignore name changes to prevent recreation on every apply
  }
}
