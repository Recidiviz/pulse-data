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
"""Configured metrics for custom workflows impact metrics displayable in looker and responsive to task_type parameter"""

from recidiviz.aggregated_metrics.models import (
    aggregated_metric_configurations as metric_configs,
)
from recidiviz.aggregated_metrics.models.aggregated_metric import AggregatedMetric
from recidiviz.aggregated_metrics.models.aggregated_metric_configurations import (
    DEDUPED_TASK_COMPLETION_EVENT_VB,
)
from recidiviz.aggregated_metrics.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)
from recidiviz.common.decarceral_impact_type import DecarceralImpactType
from recidiviz.common.str_field_utils import snake_to_title

WORKFLOWS_ASSIGNMENT_NAMES_TO_TYPES = {
    "ALL_JUSTICE_INVOLVED_STATES": (
        MetricPopulationType.JUSTICE_INVOLVED,
        MetricUnitOfAnalysisType.ALL_STATES,
    ),
    "JUSTICE_INVOLVED_STATE": (
        MetricPopulationType.JUSTICE_INVOLVED,
        MetricUnitOfAnalysisType.STATE_CODE,
    ),
    "JUSTICE_INVOLVED_LOCATION": (
        MetricPopulationType.JUSTICE_INVOLVED,
        MetricUnitOfAnalysisType.LOCATION,
    ),
    "JUSTICE_INVOLVED_LOCATION_DETAIL": (
        MetricPopulationType.JUSTICE_INVOLVED,
        MetricUnitOfAnalysisType.LOCATION_DETAIL,
    ),
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
    "ALL_INCARCERATION_STATES": (
        MetricPopulationType.INCARCERATION,
        MetricUnitOfAnalysisType.ALL_STATES,
    ),
    "INCARCERATION_FACILITY": (
        MetricPopulationType.INCARCERATION,
        MetricUnitOfAnalysisType.FACILITY,
    ),
    "INCARCERATION_FACILITY_COUNSELOR": (
        MetricPopulationType.INCARCERATION,
        MetricUnitOfAnalysisType.FACILITY_COUNSELOR,
    ),
}

WORKFLOWS_JSON_FIELD_FILTERS_WITH_SUGGESTIONS = {
    "task_type": sorted(
        snake_to_title(builder.task_type_name)
        for builder in DEDUPED_TASK_COMPLETION_EVENT_VB
    ),
    "system_type": ["Incarceration", "Supervision"],
    "decarceral_impact_type": sorted(
        snake_to_title(decarceral_impact_type.name)
        for decarceral_impact_type in DecarceralImpactType
    ),
    "is_jii_decarceral_transition": ["True", "False"],
    "has_mandatory_due_date": ["True", "False"],
    "task_type_is_live": ["True", "False"],
    "task_type_is_fully_launched": ["True", "False"],
    "denial_reasons": [],
}


WORKFLOWS_IMPACT_LOOKER_METRICS: list[AggregatedMetric] = [
    metric_configs.AVG_DAILY_POPULATION_TASK_ELIGIBLE,
    *metric_configs.AVG_DAILY_POPULATION_TASK_ELIGIBLE_FUNNEL_METRICS,
    metric_configs.AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_UNVIEWED_30_DAYS,
    metric_configs.AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_UNVIEWED,
    metric_configs.AVG_DAILY_POPULATION_TASK_ALMOST_ELIGIBLE,
    *metric_configs.AVG_DAILY_POPULATION_TASK_ALMOST_ELIGIBLE_FUNNEL_METRICS,
    metric_configs.DISTINCT_PROVISIONED_WORKFLOWS_USERS,
    metric_configs.DISTINCT_REGISTERED_PROVISIONED_WORKFLOWS_USERS,
    metric_configs.DISTINCT_PROVISIONED_PRIMARY_WORKFLOWS_USERS,
    metric_configs.DISTINCT_REGISTERED_PRIMARY_WORKFLOWS_USERS,
    metric_configs.DISTINCT_LOGGED_IN_PRIMARY_WORKFLOWS_USERS,
    metric_configs.DISTINCT_ACTIVE_PRIMARY_WORKFLOWS_USERS,
    metric_configs.LOGINS_BY_PRIMARY_WORKFLOWS_USER,
    metric_configs.PERSON_DAYS_TASK_ELIGIBLE,
    metric_configs.TASK_COMPLETIONS,
    metric_configs.TASK_COMPLETIONS_WHILE_ALMOST_ELIGIBLE,
    metric_configs.TASK_COMPLETIONS_WHILE_ELIGIBLE,
    metric_configs.TASK_COMPLETIONS_AFTER_TOOL_ACTION,
    metric_configs.TASK_COMPLETIONS_WHILE_ALMOST_ELIGIBLE_AFTER_TOOL_ACTION,
    metric_configs.DAYS_ELIGIBLE_AT_TASK_COMPLETION,
    metric_configs.TASK_ELIGIBILITY_STARTS_WHILE_ALMOST_ELIGIBLE_AFTER_TOOL_ACTION,
    metric_configs.FIRST_TOOL_ACTIONS,
    metric_configs.DAYS_ELIGIBLE_AT_FIRST_TOOL_ACTION,
    metric_configs.DISTINCT_OFFICERS_WITH_CANDIDATE_CASELOAD,
    metric_configs.DISTINCT_OFFICERS_WITH_ELIGIBLE_OR_ALMOST_ELIGIBLE_CASELOAD,
    metric_configs.DISTINCT_OFFICERS_WITH_ELIGIBLE_CASELOAD,
    metric_configs.DISTINCT_OFFICERS_WITH_ALMOST_ELIGIBLE_CASELOAD,
    metric_configs.DISTINCT_OFFICERS_WITH_TASKS_COMPLETED,
    metric_configs.DISTINCT_OFFICERS_WITH_TASKS_COMPLETED_WHILE_ELIGIBLE_OR_ALMOST_ELIGIBLE,
    metric_configs.DISTINCT_OFFICERS_WITH_TASKS_COMPLETED_WHILE_ELIGIBLE,
    metric_configs.DISTINCT_OFFICERS_WITH_TASKS_COMPLETED_WHILE_ALMOST_ELIGIBLE,
    metric_configs.DISTINCT_OFFICERS_WITH_TASKS_COMPLETED_AFTER_TOOL_ACTION,
]
