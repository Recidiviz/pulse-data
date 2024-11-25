# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Defines a map of population type to the units of analysis that metrics should be
generated for when generating our standard set of aggregated metrics.
"""
from typing import Dict, List

from recidiviz.aggregated_metrics.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)

UNIT_OF_ANALYSIS_TYPES_BY_POPULATION_TYPE: Dict[
    MetricPopulationType, List[MetricUnitOfAnalysisType]
] = {
    MetricPopulationType.INCARCERATION: [
        MetricUnitOfAnalysisType.FACILITY,
        MetricUnitOfAnalysisType.FACILITY_COUNSELOR,
        MetricUnitOfAnalysisType.STATE_CODE,
    ],
    MetricPopulationType.SUPERVISION: [
        MetricUnitOfAnalysisType.SUPERVISION_OFFICER,
        MetricUnitOfAnalysisType.SUPERVISION_UNIT,
        MetricUnitOfAnalysisType.SUPERVISION_OFFICE,
        MetricUnitOfAnalysisType.SUPERVISION_DISTRICT,
        MetricUnitOfAnalysisType.STATE_CODE,
    ],
    MetricPopulationType.JUSTICE_INVOLVED: [
        MetricUnitOfAnalysisType.STATE_CODE,
        MetricUnitOfAnalysisType.FACILITY,
        MetricUnitOfAnalysisType.SUPERVISION_DISTRICT,
    ],
}
