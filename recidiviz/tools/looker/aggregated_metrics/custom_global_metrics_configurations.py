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
"""Configured metrics for custom global usage impact metrics displayable in Looker"""

import recidiviz.aggregated_metrics.models.aggregated_metric_configurations as metric_config
from recidiviz.aggregated_metrics.models.aggregated_metric import AggregatedMetric
from recidiviz.aggregated_metrics.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)

GLOBAL_ASSIGNMENT_NAMES_TO_TYPES = {
    "ALL_STATES": (
        MetricPopulationType.JUSTICE_INVOLVED,
        MetricUnitOfAnalysisType.ALL_STATES,
    ),
    "ALL_SUPERVISION_STATES": (
        MetricPopulationType.SUPERVISION,
        MetricUnitOfAnalysisType.ALL_STATES,
    ),
    "ALL_INCARCERATION_STATES": (
        MetricPopulationType.INCARCERATION,
        MetricUnitOfAnalysisType.ALL_STATES,
    ),
    "JUSTICE_INVOLVED_STATE": (
        MetricPopulationType.JUSTICE_INVOLVED,
        MetricUnitOfAnalysisType.STATE_CODE,
    ),
    "SUPERVISION_DISTRICT": (
        MetricPopulationType.SUPERVISION,
        MetricUnitOfAnalysisType.SUPERVISION_DISTRICT,
    ),
    "SUPERVISION_OFFICE": (
        MetricPopulationType.SUPERVISION,
        MetricUnitOfAnalysisType.SUPERVISION_OFFICE,
    ),
    "SUPERVISION_UNIT_SUPERVISOR": (
        MetricPopulationType.SUPERVISION,
        MetricUnitOfAnalysisType.SUPERVISION_UNIT,
    ),
    "SUPERVISION_OFFICER": (
        MetricPopulationType.SUPERVISION,
        MetricUnitOfAnalysisType.SUPERVISION_OFFICER,
    ),
    "INCARCERATION_FACILITY": (
        MetricPopulationType.INCARCERATION,
        MetricUnitOfAnalysisType.FACILITY,
    ),
    "INCARCERATION_FACILITY_COUNSELOR": (
        MetricPopulationType.INCARCERATION,
        MetricUnitOfAnalysisType.FACILITY_COUNSELOR,
    ),
    "GLOBAL_PROVISIONED_USER": (
        MetricPopulationType.JUSTICE_INVOLVED,
        MetricUnitOfAnalysisType.GLOBAL_PROVISIONED_USER,
    ),
}

GLOBAL_IMPACT_LOOKER_METRICS: list[AggregatedMetric] = [
    metric_config.AVG_DAILY_POPULATION,
    metric_config.DISTINCT_GLOBAL_PROVISIONED_USERS,
    metric_config.DISTINCT_REGISTERED_GLOBAL_PROVISIONED_USERS,
    metric_config.DISTINCT_LOGGED_IN_GLOBAL_USERS,
    metric_config.LOGINS_GLOBAL_USER,
    metric_config.DISTINCT_ACTIVE_GLOBAL_USER_ALL_TOOLS,
    metric_config.ACTIVE_USAGE_EVENTS_GLOBAL_USER,
    metric_config.DISTINCT_PROVISIONED_PRIMARY_WORKFLOWS_USERS,
    metric_config.DISTINCT_REGISTERED_PRIMARY_WORKFLOWS_USERS,
    metric_config.DISTINCT_ACTIVE_WORKFLOWS_PRIMARY_USERS_ALL_TOOLS,
]
