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
"""Configured metrics for custom tasks impact metrics displayable in Looker"""

import recidiviz.aggregated_metrics.models.aggregated_metric_configurations as metric_config
from recidiviz.aggregated_metrics.models.aggregated_metric import AggregatedMetric
from recidiviz.aggregated_metrics.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)

TASKS_ASSIGNMENT_NAMES_TO_TYPES = {
    "ALL_SUPERVISION_STATES": (
        MetricPopulationType.SUPERVISION,
        MetricUnitOfAnalysisType.ALL_STATES,
    ),
    "SUPERVISION_STATE": (
        MetricPopulationType.SUPERVISION,
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
}

TASKS_JSON_FIELD_FILTERS_WITH_SUGGESTIONS: dict[str, list[str]] = {
    "tasks_contact_type": [],
}


TASKS_IMPACT_LOOKER_METRICS: list[AggregatedMetric] = [
    metric_config.AVG_DAILY_POPULATION,
    metric_config.DISTINCT_PROVISIONED_TASKS_USERS,
    metric_config.DISTINCT_REGISTERED_PROVISIONED_TASKS_USERS,
    metric_config.DISTINCT_PROVISIONED_PRIMARY_TASKS_USERS,
    metric_config.DISTINCT_REGISTERED_PRIMARY_TASKS_USERS,
    metric_config.DISTINCT_LOGGED_IN_PRIMARY_TASKS_USERS,
    metric_config.DISTINCT_ACTIVE_PRIMARY_TASKS_USERS,
    metric_config.LOGINS_PRIMARY_TASKS_USERS,
    metric_config.AVG_DAILY_POPULATION_ASSESSMENT_REQUIRED,
    metric_config.AVG_DAILY_POPULATION_ASSESSMENT_OVERDUE,
    metric_config.CONTACT_DUE_DATES,
    metric_config.CONTACT_DUE_DATES_MET,
    metric_config.CONTACTS_ATTEMPTED,
    metric_config.CONTACTS_COMPLETED,
]
