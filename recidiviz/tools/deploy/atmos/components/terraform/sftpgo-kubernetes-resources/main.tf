locals {
  sftpgo_namespace = "sftpgo"
  sftpgo_version = "0.37.0"
}

resource "google_compute_address" "default" {
  name   = "sftpgo-static-ip-address"
  region = var.region
  project = var.project_id
}

resource "kubernetes_namespace" "sftpgo" {
  metadata {
    name = local.sftpgo_namespace
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

  volume_binding_mode = "WaitForFirstConsumer"
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
    name = "persistence.enabled"
    value = true
  }

  set {
    name = "persistence.pvc.storageClassName"
    value = kubernetes_storage_class.hyperdisk.metadata[0].name
  }

  set {
    name = "persistence.pvc.resources.requests.storage"
    value = "25Gi"
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
