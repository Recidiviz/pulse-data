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

resource "google_app_engine_application_url_dispatch_rules" "request_routes" {
  # Scrapers
  dispatch_rules {
    domain  = "*"
    path    = "/batch/*"
    service = "scrapers"
  }
  dispatch_rules {
    domain  = "*"
    path    = "/infer_release/*"
    service = "scrapers"
  }
  dispatch_rules {
    domain  = "*"
    path    = "/ingest/*"
    service = "scrapers"
  }
  dispatch_rules {
    domain  = "*"
    path    = "/scraper/*"
    service = "scrapers"
  }
  dispatch_rules {
    domain  = "*"
    path    = "/scrape_aggregate_reports/*"
    service = "scrapers"
  }
  dispatch_rules {
    domain  = "*"
    path    = "/single_count/*"
    service = "scrapers"
  }

  # Default
  dispatch_rules {
    domain  = "*"
    path    = "/*"
    service = "default"
  }
}
