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
"""Various BigQuery datasets."""

# Transitional dataset in the same region (e.g. us-east1) as the Operations CloudSQL
# instance where Operations CloudSQL data is stored before the CloudSQL -> BQ refresh
# copies it to a dataset in the 'US' multi-region.
OPERATIONS_BASE_REGIONAL_DATASET: str = "operations_regional"

# Where the base tables for the operations schema live. These are a mirror of the data
# in our operations CloudSQL instance, refreshed daily via the CloudSQL -> BQ federated
# export.
OPERATIONS_BASE_DATASET: str = "operations"
