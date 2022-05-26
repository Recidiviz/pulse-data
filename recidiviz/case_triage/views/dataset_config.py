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
"""Dataset configuration for case triage."""

CASE_TRIAGE_DATASET: str = "case_triage"

# Transitional dataset in the same region (e.g. us-east1) as the Case Triage CloudSQL
# instance where Case Triage CloudSQL data is stored before the CloudSQL -> BQ refresh
# copies it to a dataset in the 'US' multi-region.
CASE_TRIAGE_FEDERATED_REGIONAL_DATASET: str = "case_triage_federated_regional"

# Where data exported from CloudSQL -> BQ federated export lives
CASE_TRIAGE_FEDERATED_DATASET: str = "case_triage_federated"

# Location the Cloud SQL connection resides in
CASE_TRIAGE_CLOUDSQL_LOCATION: str = "us-central1"

# CloudSQL connection dataset which allows us to query Case Triage Postgres directly
CASE_TRIAGE_CLOUDSQL_CONNECTION: str = "case_triage_cloudsql"
