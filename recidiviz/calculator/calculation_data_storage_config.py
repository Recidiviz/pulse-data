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
"""Calculation data storage configuration."""

# The maximum number days of output that should be stored in a dataflow metrics table before being moved to cold storage
MAX_DAYS_IN_DATAFLOW_METRICS_TABLE: int = 7

# Where the metrics from outdated Dataflow jobs are stored
DATAFLOW_METRICS_COLD_STORAGE_DATASET: str = 'dataflow_metrics_cold_storage'
