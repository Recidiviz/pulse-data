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

locals {
  # SFTPGo includes some IP-checking mechanism as part of its web CSRF token validation
  # These values are needed to allow for proxied request to pass CSRF validation
  # https://github.com/drakkan/sftpgo/issues/1816
  allowed_proxy_env_vars = [for index, value  in local.load_balancing_ips :{
    name = format("env.SFTPGO_HTTPD__BINDINGS__0__PROXY_ALLOWED__%s", index)
    value = value
  }]
}

resource "helm_release" "sftpgo" {
  name      = "sftpgo"
  namespace = kubernetes_namespace.sftpgo.metadata[0].name
  chart     = "oci://ghcr.io/sftpgo/helm-charts/sftpgo"
  version   = local.sftpgo_version
  wait      = true

  values = [file("values.yaml")]

  set = concat(
    [
      {
        name = "services.sftp-public.loadBalancerIp",
        value = google_compute_address.default.address
      },
      {
        name  = "serviceAccount.name"
        value = kubernetes_service_account.sftpgo.metadata[0].name
      },
      {
        name  = "serviceAccount.create"
        value = false
      },
      {
        name  = "persistence.enabled"
        value = true
      },
      {
        name  = "persistence.pvc.storageClassName"
        value = kubernetes_storage_class.hyperdisk.metadata[0].name
      },
      {
        name  = "persistence.pvc.resources.requests.storage"
        value = "25Gi"
      },
      {
        name  = "env.SFTPGO_DEFAULT_ADMIN_USERNAME"
        value = local.sftpgo_admin
      },
      {
        name  = "env.SFTPGO_DEFAULT_ADMIN_PASSWORD"
        value = local.sftpgo_admin_password
      },
      {
        name = "env.SFTPGO_LOG_LEVEL",
        value = "debug"
      }
    ],
    local.allowed_proxy_env_vars
  )

  upgrade_install = true
}

# Create virtual folder resources
resource "sftpgo_folder" "virtual_folders" {
  for_each = {
    for folder in var.sftpgo_virtual_folders :
    folder.name => folder
  }

  depends_on = [helm_release.sftpgo]

  name = each.value.name

  filesystem = {
    provider = 2  # Google Cloud Storage
    gcsconfig = {
      bucket                = each.value.bucket
      automatic_credentials = 1
    }
  }
}

resource "sftpgo_user" "sftp_user" {
  depends_on = [helm_release.sftpgo, sftpgo_folder.virtual_folders]

  username = "${local.lower_state_abbr}-sftp"

  filesystem = {
    provider = 2
    gcsconfig = {
      bucket                = var.sftp_bucket_name
      automatic_credentials = 1
    }
  }

  # Reference the created virtual folders
  virtual_folders = [
    for folder in var.sftpgo_virtual_folders : {
      name         = folder.name
      virtual_path = folder.mount_path
      quota_files  = 0  # 0 means unlimited
      quota_size   = 0  # 0 means unlimited
    }
  ]

  home_dir = "/tmp/${local.lower_state_abbr}-sftp"

  # Merge permissions: root directory + virtual folder paths
  # Convert permission lists to comma-separated strings as SFTPGo expects
  permissions = merge(
    { "/" = "*" },
    {
      for folder in var.sftpgo_virtual_folders :
      folder.mount_path => join(",", folder.permissions)
    }
  )

  password    = local.sftpgo_user_password
  public_keys = local.sftpgo_user_public_key != "" ? [local.sftpgo_user_public_key] : []
  status      = 1 # enabled
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

# Grant read access to virtual folder buckets
# All virtual folders get at least read access
resource "google_storage_bucket_iam_member" "sftp-virtual-folder-viewer" {
  for_each = {
    for folder in var.sftpgo_virtual_folders :
    folder.name => folder
  }

  bucket = each.value.bucket
  role   = "roles/storage.objectViewer"
  member = "principal://iam.googleapis.com/projects/${data.google_project.project.number}/locations/global/workloadIdentityPools/${var.project_id}.svc.id.goog/subject/ns/${kubernetes_namespace.sftpgo.metadata[0].name}/sa/${kubernetes_service_account.sftpgo.metadata[0].name}"
}

# Grant write access to virtual folder buckets (only if permissions include write capabilities)
resource "google_storage_bucket_iam_member" "sftp-virtual-folder-creator" {
  for_each = {
    for folder in var.sftpgo_virtual_folders :
    folder.name => folder
    if contains(folder.permissions, "*") || contains(folder.permissions, "upload") || contains(folder.permissions, "overwrite")
  }

  bucket = each.value.bucket
  role   = "roles/storage.objectCreator"
  member = "principal://iam.googleapis.com/projects/${data.google_project.project.number}/locations/global/workloadIdentityPools/${var.project_id}.svc.id.goog/subject/ns/${kubernetes_namespace.sftpgo.metadata[0].name}/sa/${kubernetes_service_account.sftpgo.metadata[0].name}"
}

# Grant full object access (create + delete) to virtual folder buckets when the
# user can rename/move or delete files. On GCS a rename is implemented as a
# copy + delete, so the service account needs both create and delete on objects
# (neither objectViewer nor objectCreator grants delete).
resource "google_storage_bucket_iam_member" "sftp-virtual-folder-object-user" {
  for_each = {
    for folder in var.sftpgo_virtual_folders :
    folder.name => folder
    if contains(folder.permissions, "*") || contains(folder.permissions, "delete") || contains(folder.permissions, "rename") || contains(folder.permissions, "rename_files") || contains(folder.permissions, "rename_dirs")
  }

  bucket = each.value.bucket
  role   = "roles/storage.objectUser"
  member = "principal://iam.googleapis.com/projects/${data.google_project.project.number}/locations/global/workloadIdentityPools/${var.project_id}.svc.id.goog/subject/ns/${kubernetes_namespace.sftpgo.metadata[0].name}/sa/${kubernetes_service_account.sftpgo.metadata[0].name}"
}

data "google_container_cluster" "primary" {
  name     = "sftpgo-cluster"
  location = var.zone
}

# ClusterIP service for admin HTTP traffic (for load balancer)
# The annotation tells GKE to create and manage the NEG
resource "kubernetes_service_v1" "sftpgo_admin_http" {
  depends_on = [helm_release.sftpgo]

  metadata {
    name      = "sftpgo-admin-http"
    namespace = kubernetes_namespace.sftpgo.metadata[0].name
    annotations = {
      "cloud.google.com/neg" = jsonencode({
        "exposed_ports" = {
          "80" = { "name" = "sftpgo-admin-neg-l7" }
        }
      })
    }
  }

  spec {
    type = "ClusterIP"

    selector = {
      "app.kubernetes.io/name" = "sftpgo"
    }

    port {
      name        = "http"
      port        = 80
      target_port = 8080
      protocol    = "TCP"
    }
  }

  # Ignore the cloud.google.com/neg-status annotation that GKE automatically adds
  lifecycle {
    ignore_changes = [
      metadata[0].annotations["cloud.google.com/neg-status"]
    ]
  }
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
  kubernetes = {
    host                   = "https://${var.kubernetes_endpoint}"
    cluster_ca_certificate = base64decode(var.kubernetes_ca_certificate)
    token                  = data.google_client_config.provider.access_token
    exec = {
      api_version = "client.authentication.k8s.io/v1beta1"
      command     = "gke-gcloud-auth-plugin"
    }
  }
}

provider "sftpgo" {
  host     = "http://${google_compute_address.default.address}:8080"
  username = local.sftpgo_admin
  password = local.sftpgo_admin_password
}
