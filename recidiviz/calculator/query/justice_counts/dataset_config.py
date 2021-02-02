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

# Where the base tables for the JusticeCounts schema live
JUSTICE_COUNTS_BASE_DATASET: str = 'justice_counts'

# Where the calculations for Corrections data live
JUSTICE_COUNTS_CORRECTIONS_DATASET: str = 'justice_counts_corrections'

# Where the views that are exported to the dashboard live
JUSTICE_COUNTS_DASHBOARD_DATASET: str = 'justice_counts_dashboard'
