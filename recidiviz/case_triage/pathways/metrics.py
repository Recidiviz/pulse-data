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
# ============================================================================
""" Contains the configuration for which Pathways metrics are enabled """

from typing import Dict, List

from recidiviz.case_triage.pathways.metric_queries import (
    CountByDimensionMetricQueryBuilder,
    LibertyToPrisonTransitionsCount,
    MetricQueryBuilder,
    PersonLevelMetricQueryBuilder,
    PrisonToSupervisionTransitionsCount,
    PrisonToSupervisionTransitionsPersonLevel,
    SupervisionToLibertyTransitionsCount,
    SupervisionToPrisonTransitionsCount,
)
from recidiviz.common.constants.states import StateCode

ALL_METRICS: List[MetricQueryBuilder] = [
    LibertyToPrisonTransitionsCount,
    PrisonToSupervisionTransitionsCount,
    PrisonToSupervisionTransitionsPersonLevel,
    SupervisionToLibertyTransitionsCount,
    SupervisionToPrisonTransitionsCount,
]

ENABLED_METRICS_BY_STATE: Dict[StateCode, List[MetricQueryBuilder]] = {
    StateCode.US_ID: ALL_METRICS,
    StateCode.US_ME: ALL_METRICS,
    StateCode.US_ND: ALL_METRICS,
    StateCode.US_MO: ALL_METRICS,
    StateCode.US_TN: ALL_METRICS,
}

ENABLED_METRICS_BY_STATE_BY_NAME = {
    state_code: {metric.name: metric for metric in metrics}
    for state_code, metrics in ENABLED_METRICS_BY_STATE.items()
}

ENABLED_COUNT_BY_DIMENSION_METRICS_BY_STATE = {
    state_code: [
        metric_mapper
        for metric_mapper in metric_mappers
        if isinstance(metric_mapper, CountByDimensionMetricQueryBuilder)
    ]
    for state_code, metric_mappers in ENABLED_METRICS_BY_STATE.items()
}

ENABLED_PERSON_LEVEL_METRICS_BY_STATE = {
    state_code: [
        metric_mapper
        for metric_mapper in metric_mappers
        if isinstance(metric_mapper, PersonLevelMetricQueryBuilder)
    ]
    for state_code, metric_mappers in ENABLED_METRICS_BY_STATE.items()
}
