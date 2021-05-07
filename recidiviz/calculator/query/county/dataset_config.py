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

"""County-level dataset configuration."""

VIEWS_DATASET: str = "census_views"

# Transitional dataset in the same region (e.g. us-east1) as the Jails CloudSQL
# instance where Jails CloudSQL data is stored before the CloudSQL -> BQ export
# copies it to a dataset in the 'US' multi-region.
COUNTY_BASE_REGIONAL_DATASET: str = "census_regional"

# Where data exported from CloudSQL -> BQ federated export lives
COUNTY_BASE_DATASET: str = "census"
