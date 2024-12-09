# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Dataset configuration for aggregated metrics views"""


# Dataset defining views that join unit of analysis assignment sessions views (views
# that connect units of analysis with units of observation for periods of time) to
# metric time periods (e.g. year-long, month-long, etc) for all relevant populations,
# units of analysis, etc that our deployed aggregated metrics collections will
# reference.
UNIT_OF_ANALYSIS_ASSIGNMENTS_BY_TIME_PERIOD_DATASET_ID = (
    "unit_of_analysis_assignments_by_time_period"
)

# Dataset with views that define populations, time periods, and metrics at various
# levels of aggregation
AGGREGATED_METRICS_DATASET_ID = "aggregated_metrics"
