resource "google_compute_network" "sftpgo_network" {
  name                    = "sftpgo-network"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "sftpgo_subnetwork" {
  name          = "sftpgo-subnet"
  ip_cidr_range = "10.0.0.0/17"
  region        = var.region
  network       = google_compute_network.sftpgo_network.name

  secondary_ip_range {
    range_name    = "ip-range-pods"
    ip_cidr_range = "192.168.0.0/18"
  }

  secondary_ip_range {
    range_name    = "ip-range-services"
    ip_cidr_range = "192.168.64.0/18"
  }
}

module "runner-cluster" {
  source                   = "../vendor/gke-public-cluster"
  project_id               = var.project_id
  name                     = "sftpgo-cluster"
  regional                 = false
  region                   = var.region
  zones                    = var.zones
  network                  = google_compute_network.sftpgo_network.name
  network_project_id       = var.project_id
  subnetwork               = google_compute_subnetwork.sftpgo_subnetwork.name
  ip_range_pods            = "ip-range-pods"
  ip_range_services        = "ip-range-services"
  logging_service          = "logging.googleapis.com/kubernetes"
  monitoring_service       = "monitoring.googleapis.com/kubernetes"
  remove_default_node_pool = true
  service_account          = "create"
  gce_pd_csi_driver        = true
  deletion_protection = false
  # Enable Filestore driver (used by runner data cache)
  filestore_csi_driver     = true

  cluster_autoscaling = {
    enabled             = true
    min_cpu_cores       = 2
    max_cpu_cores       = 8
    min_memory_gb       = 8
    max_memory_gb       = 32
    auto_repair         = true
    auto_upgrade        = true
    autoscaling_profile = "OPTIMIZE_UTILIZATION"
    gpu_resources = []
  }

  node_pools = [
    {
      name         = "sftpgo-node-pool",
      autoscaling  = true
      min_count    = 1
      max_count    = 4
      machine_type = "c4-highcpu-2"
      disk_type    = "hyperdisk-balanced"
    }
  ]
}
