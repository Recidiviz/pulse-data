# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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

resource "google_redis_instance" "case_triage_rate_limiter_cache" {
  name           = "rate-limit-cache"
  region         = var.us_east_region
  memory_size_gb = 1
  tier           = "BASIC"
  redis_version  = "REDIS_5_0"
}

resource "google_redis_instance" "case_triage_sessions_cache" {
  name           = "case-triage-sessions-cache"
  region         = var.us_east_region
  memory_size_gb = 1
  tier           = "BASIC"
  redis_version  = "REDIS_5_0"
}

resource "google_redis_instance" "pathways_metric_cache" {
  name           = "pathways-metric-cache"
  region         = var.us_east_region
  memory_size_gb = 1
  tier           = "BASIC"
  redis_version  = "REDIS_5_0"
}

resource "google_redis_instance" "admin_panel_cache" {
  name           = "admin-panel-cache"
  region         = var.us_central_region
  memory_size_gb = 1
  tier           = "BASIC"
  redis_version  = "REDIS_5_0"
}

resource "google_redis_instance" "public_pathways_metric_cache" {
  name           = "public-pathways-metric-cache"
  region         = var.us_central_region
  memory_size_gb = 1
  tier           = "BASIC"
  redis_version  = "REDIS_5_0"
}


# Store host in a secret

resource "google_secret_manager_secret" "case_triage_rate_limiter_redis_host" {
  secret_id = "case_triage_rate_limiter_redis_host"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "case_triage_rate_limiter_redis_host" {
  secret      = google_secret_manager_secret.case_triage_rate_limiter_redis_host.name
  secret_data = google_redis_instance.case_triage_rate_limiter_cache.host
}

resource "google_secret_manager_secret" "case_triage_sessions_redis_host" {
  secret_id = "case_triage_sessions_redis_host"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "case_triage_sessions_redis_host" {
  secret      = google_secret_manager_secret.case_triage_sessions_redis_host.name
  secret_data = google_redis_instance.case_triage_rate_limiter_cache.host
}


resource "google_secret_manager_secret" "pathways_metric_redis_host" {
  secret_id = "pathways_metric_redis_host"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "pathways_metric_redis_host" {
  secret      = google_secret_manager_secret.pathways_metric_redis_host.name
  secret_data = google_redis_instance.pathways_metric_cache.host
}


resource "google_secret_manager_secret" "admin_panel_redis_host" {
  secret_id = "admin_panel_redis_host"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "admin_panel_redis_host" {
  secret      = google_secret_manager_secret.admin_panel_redis_host.name
  secret_data = google_redis_instance.admin_panel_cache.host
}

resource "google_secret_manager_secret" "public_pathways_metric_redis_host" {
  secret_id = "public_pathways_metric_redis_host"
  labels = {
    label = "Public Pathways Metric Redis Host"
  }

  replication {
    user_managed {
      replicas {
        location = "us-central1"
      }
      replicas {
        location = "us-east1"
      }
    }
  }
}

resource "google_secret_manager_secret_version" "public_pathways_metric_redis_host" {
  secret      = google_secret_manager_secret.public_pathways_metric_redis_host.name
  secret_data = google_redis_instance.public_pathways_metric_cache.host
}

# Store port in a secret

resource "google_secret_manager_secret" "case_triage_rate_limiter_redis_port" {
  secret_id = "case_triage_rate_limiter_redis_port"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "case_triage_rate_limiter_redis_port" {
  secret      = google_secret_manager_secret.case_triage_rate_limiter_redis_port.name
  secret_data = google_redis_instance.case_triage_rate_limiter_cache.port
}


resource "google_secret_manager_secret" "case_triage_sessions_redis_port" {
  secret_id = "case_triage_sessions_redis_port"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "case_triage_sessions_redis_port" {
  secret      = google_secret_manager_secret.case_triage_sessions_redis_port.name
  secret_data = google_redis_instance.case_triage_sessions_cache.port
}

resource "google_secret_manager_secret" "pathways_metric_redis_port" {
  secret_id = "pathways_metric_redis_port"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "pathways_metric_redis_port" {
  secret      = google_secret_manager_secret.pathways_metric_redis_port.name
  secret_data = google_redis_instance.pathways_metric_cache.port
}


resource "google_secret_manager_secret" "admin_panel_redis_port" {
  secret_id = "admin_panel_redis_port"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "admin_panel_redis_port" {
  secret      = google_secret_manager_secret.admin_panel_redis_port.name
  secret_data = google_redis_instance.admin_panel_cache.port
}

resource "google_secret_manager_secret" "public_pathways_metric_redis_port" {
  secret_id = "public_pathways_metric_redis_port"
  labels = {
    label = "Public Pathways Metric Redis Port"
  }

  replication {
    user_managed {
      replicas {
        location = "us-central1"
      }
      replicas {
        location = "us-east1"
      }
    }
  }
}

resource "google_secret_manager_secret_version" "public_pathways_metric_redis_port" {
  secret      = google_secret_manager_secret.public_pathways_metric_redis_port.name
  secret_data = google_redis_instance.public_pathways_metric_cache.port
}
