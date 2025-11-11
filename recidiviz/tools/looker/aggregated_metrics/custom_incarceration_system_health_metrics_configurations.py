# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""
Additional configurations for autogeneration of LookML for a custom aggregated
metrics explore for incarceration system health metrics
"""

from recidiviz.aggregated_metrics.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)

INCARCERATION_SYSTEM_HEALTH_ASSIGNMENT_NAMES_TO_TYPES = {
    "ALL_INCARCERATION_STATES": (
        MetricPopulationType.INCARCERATION,
        MetricUnitOfAnalysisType.ALL_STATES,
    ),
    "INCARCERATION_STATE": (
        MetricPopulationType.INCARCERATION,
        MetricUnitOfAnalysisType.STATE_CODE,
    ),
    "FACILITY": (
        MetricPopulationType.INCARCERATION,
        MetricUnitOfAnalysisType.FACILITY,
    ),
    "FACILITY_COUNSELOR": (
        MetricPopulationType.INCARCERATION,
        MetricUnitOfAnalysisType.FACILITY_COUNSELOR,
    ),
}
