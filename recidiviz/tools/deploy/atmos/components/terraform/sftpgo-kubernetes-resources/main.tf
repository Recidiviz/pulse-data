locals {
  sftpgo_namespace = "sftpgo"
  sftpgo_version   = "0.37.0"
  sftpgo_admin     = "recidiviz"
  lower_state_code = replace(lower(var.state_code), "_", "-")
  lower_state_abbr = split("_", lower(var.state_code))[1]
}

data "google_project" "project" {
}


resource "google_compute_address" "default" {
  name    = "sftpgo-static-ip-address"
  region  = var.region
  project = var.project_id
}

resource "kubernetes_namespace" "sftpgo" {
  metadata {
    name = local.sftpgo_namespace
  }
}

resource "kubernetes_service_account" "sftpgo" {
  metadata {
    name      = "${local.lower_state_code}-sftpgo"
    namespace = kubernetes_namespace.sftpgo.metadata[0].name
  }
}

# https://cloud.google.com/kubernetes-engine/docs/how-to/persistent-volumes/hyperdisk#create-storageclass
resource "kubernetes_storage_class" "hyperdisk" {
  storage_provisioner = "pd.csi.storage.gke.io"

  parameters = {
    type                             = "hyperdisk-balanced"
    provisioned-throughput-on-create = "250Mi"
    provisioned-iops-on-create       = "7000"
  }

  volume_binding_mode    = "WaitForFirstConsumer"
  allow_volume_expansion = true

  metadata {
    name = "balanced-storage"
  }
}

resource "helm_release" "sftpgo" {
  name      = "sftpgo"
  namespace = kubernetes_namespace.sftpgo.metadata[0].name
  chart     = "oci://ghcr.io/sftpgo/helm-charts/sftpgo"
  version   = local.sftpgo_version
  wait      = true

  values = [replace(file("values.yaml"), "!!LOADBALANCERIP!!", google_compute_address.default.address)]

  set {
    name  = "serviceAccount.name"
    value = kubernetes_service_account.sftpgo.metadata[0].name
  }

  set {
    name  = "serviceAccount.create"
    value = false
  }

  set {
    name  = "persistence.enabled"
    value = true
  }

  set {
    name  = "persistence.pvc.storageClassName"
    value = kubernetes_storage_class.hyperdisk.metadata[0].name
  }

  set {
    name  = "persistence.pvc.resources.requests.storage"
    value = "25Gi"
  }

  set {
    name  = "env.SFTPGO_DEFAULT_ADMIN_USERNAME"
    value = local.sftpgo_admin
  }
  set {
    name  = "env.SFTPGO_DEFAULT_ADMIN_PASSWORD"
    value = data.google_secret_manager_secret_version.sftpgo_admin_password.secret_data
  }
}


data "google_secret_manager_secret_version" "sftpgo_admin_password" {
  secret = "sftpgo_admin_password"
}

data "google_secret_manager_secret_version" "sftpgo_user_password" {
  secret = "sftpgo_${local.lower_state_code}_password"
}


resource "sftpgo_user" "sftp_user" {
  depends_on = [helm_release.sftpgo]

  username = "${local.lower_state_abbr}-sftp"

  filesystem = {
    provider = 2
    gcsconfig = {
      bucket                = var.sftp_bucket_name
      automatic_credentials = 1
    }
  }

  home_dir = "/tmp/${local.lower_state_abbr}-sftp"
  permissions = {
    "/" = "*"
  }
  password = data.google_secret_manager_secret_version.sftpgo_user_password.secret_data
  status   = 1 # enabled
}

resource "google_storage_bucket_iam_member" "sftp-bucket-creator" {
  bucket = var.sftp_bucket_name
  role   = "roles/storage.objectCreator"
  member = "principal://iam.googleapis.com/projects/${data.google_project.project.number}/locations/global/workloadIdentityPools/${var.project_id}.svc.id.goog/subject/ns/${kubernetes_namespace.sftpgo.metadata[0].name}/sa/${kubernetes_service_account.sftpgo.metadata[0].name}"
}

resource "google_storage_bucket_iam_member" "sftp-bucket-viewer" {
  bucket = var.sftp_bucket_name
  role   = "roles/storage.objectViewer"
  member = "principal://iam.googleapis.com/projects/${data.google_project.project.number}/locations/global/workloadIdentityPools/${var.project_id}.svc.id.goog/subject/ns/${kubernetes_namespace.sftpgo.metadata[0].name}/sa/${kubernetes_service_account.sftpgo.metadata[0].name}"
}

# Retrieve an access token as the Terraform runner
data "google_client_config" "provider" {}


provider "kubernetes" {
  host  = "https://${var.kubernetes_endpoint}"
  token = data.google_client_config.provider.access_token
  cluster_ca_certificate = base64decode(
    var.kubernetes_ca_certificate,
  )
}

provider "helm" {
  kubernetes {
    host                   = "https://${var.kubernetes_endpoint}"
    cluster_ca_certificate = base64decode(var.kubernetes_ca_certificate)
    token                  = data.google_client_config.provider.access_token
    exec {
      api_version = "client.authentication.k8s.io/v1beta1"
      command     = "gke-gcloud-auth-plugin"
    }
  }
}

provider "sftpgo" {
  host     = "http://${google_compute_address.default.address}:8080"
  username = local.sftpgo_admin
  password = data.google_secret_manager_secret_version.sftpgo_admin_password.secret_data
}
