# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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

terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.38.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "6.38.0"
    }
    pagerduty = {
      source  = "PagerDuty/pagerduty"
      version = "3.11.4"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.us_central_region
  default_labels = {
    terraform_managed = "true"
  }
}

provider "google-beta" {
  project = var.project_id
  region  = var.us_central_region
  default_labels = {
    terraform_managed = "true"
  }
}

data "google_secret_manager_secret" "pagerduty_token" {
  secret_id = "pagerduty_terraform_key"
}

data "google_secret_manager_secret_version" "pagerduty_token" {
  secret  = data.google_secret_manager_secret.pagerduty_token.name
  version = "latest"
}

locals {
  pagerduty_token = data.google_secret_manager_secret_version.pagerduty_token.secret_data
}

provider "pagerduty" {
  # The authentication token that allows us to connect to PagerDuty. See:
  # https://registry.terraform.io/providers/PagerDuty/pagerduty/latest/docs#token
  token = local.pagerduty_token
}
