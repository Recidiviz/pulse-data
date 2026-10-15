# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================

# The VPN jump box. It has no external IP and no network tags. Humans reach it
# over IAP TCP forwarding, and the us-ne-export-sql-server-raw-data-to-gcs Cloud
# Run job reaches it over Direct VPC egress and opens an SSH tunnel through it
# to Nebraska's SQL Server (see
# recidiviz/tools/ingest/regions/us_ne/sql_to_gcs_export_config.py). The job
# reads the address it dials from the us_ne_vpn_jumpbox_ip secret.

data "google_compute_default_service_account" "default" {
  project = var.project_id
}

resource "google_compute_instance" "vpn_jump_box" {
  name    = "us-ne-vpn-jump-box"
  project = var.project_id
  zone    = var.zone

  machine_type   = "e2-medium"
  can_ip_forward = false

  # Left at the live value rather than turned on as a drive-by change; the
  # lifecycle block below is what actually stops Terraform deleting this VM.
  deletion_protection = false

  labels = {
    # Set by the Ops Agent policy that installs monitoring on this VM.
    goog-ops-agent-policy = "v2-x86-template-1-4-0"
  }

  # The boot disk is us-ne-vpn-jump-box-v2, encrypted with the jump-box-disk
  # key. device_name forces replacement, so it must match the live value.
  boot_disk {
    auto_delete       = true
    device_name       = "persistent-disk-0"
    kms_key_self_link = "projects/${var.project_id}/locations/${var.region}/keyRings/us-ne-compute/cryptoKeys/jump-box-disk"

    initialize_params {
      image = "https://www.googleapis.com/compute/v1/projects/debian-cloud/global/images/debian-12-bookworm-v20261006"
      size  = 10
      type  = "pd-balanced"
    }
  }

  network_interface {
    network    = google_compute_network.ingest_vpc.id
    subnetwork = google_compute_subnetwork.ingest_subnetwork.id
    stack_type = "IPV4_ONLY"

    # network_ip is deliberately not declared. It is Optional+Computed, so the
    # imported value stands, and the address stays out of git - it is the same
    # value the us_ne_vpn_jumpbox_ip secret holds.
  }

  # The VM also carries an ssh-keys entry that is managed outside Terraform;
  # the lifecycle block below ignores it so a plan never removes the key.
  metadata = {
    enable-osconfig = "TRUE"
  }

  # The project's default compute service account, with default scopes.
  # Replacing it is a change to make deliberately, not as part of an import.
  service_account {
    email = data.google_compute_default_service_account.default.email
    scopes = [
      "https://www.googleapis.com/auth/devstorage.read_only",
      "https://www.googleapis.com/auth/logging.write",
      "https://www.googleapis.com/auth/monitoring.write",
      "https://www.googleapis.com/auth/service.management.readonly",
      "https://www.googleapis.com/auth/servicecontrol",
      "https://www.googleapis.com/auth/trace.append",
    ]
  }

  scheduling {
    automatic_restart   = true
    on_host_maintenance = "MIGRATE"
    preemptible         = false
    provisioning_model  = "STANDARD"
  }

  shielded_instance_config {
    enable_secure_boot          = true
    enable_vtpm                 = true
    enable_integrity_monitoring = true
  }

  lifecycle {
    # Replacing this VM drops Nebraska ingest until someone re-establishes the
    # tunnel by hand, so make Terraform refuse rather than do it silently.
    prevent_destroy = true

    # initialize_params only applies when the disk is created, and any change
    # to it forces replacement, so the live disk wins.
    ignore_changes = [
      boot_disk[0].initialize_params,
      metadata["ssh-keys"],
    ]
  }
}

import {
  id = "projects/${var.project_id}/zones/${var.zone}/instances/us-ne-vpn-jump-box"
  to = google_compute_instance.vpn_jump_box
}
