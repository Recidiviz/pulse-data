# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Configured metrics for custom experiment KPI metrics displayable in Looker"""

import recidiviz.aggregated_metrics.models.aggregated_metric_configurations as metric_config
from recidiviz.aggregated_metrics.models.aggregated_metric import AggregatedMetric
from recidiviz.aggregated_metrics.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)

EXPERIMENT_KPI_ASSIGNMENT_NAMES_TO_TYPES = {
    "EXPERIMENT_COHORT": (
        MetricPopulationType.JUSTICE_INVOLVED,
        MetricUnitOfAnalysisType.EXPERIMENT_VARIANT,
    ),
    "STATE": (
        MetricPopulationType.JUSTICE_INVOLVED,
        MetricUnitOfAnalysisType.STATE_CODE,
    ),
}


EXPERIMENT_KPI_LOOKER_METRICS: list[AggregatedMetric] = [
    metric_config.AVG_DAILY_POPULATION,
    metric_config.TASK_COMPLETIONS_AFTER_TOOL_ACTION,
    metric_config.DAYS_ELIGIBLE_AT_TASK_COMPLETION,
    metric_config.TASK_COMPLETIONS_WHILE_ELIGIBLE,
    metric_config.DAYS_ELIGIBLE_AT_FIRST_TOOL_ACTION,
    metric_config.FIRST_TOOL_ACTIONS,
    metric_config.TASK_COMPLETIONS,
    metric_config.DISTINCT_ACTIVE_PRIMARY_INSIGHTS_USERS,
    metric_config.DISTINCT_ACTIVE_PRIMARY_WORKFLOWS_USERS,
    metric_config.DISTINCT_REGISTERED_PRIMARY_INSIGHTS_USERS,
    metric_config.DISTINCT_REGISTERED_PRIMARY_WORKFLOWS_USERS,
    metric_config.AVG_DAILY_POPULATION_TASK_ELIGIBLE,
    metric_config.DISTINCT_OFFICERS_WITH_ELIGIBLE_CASELOAD,
    metric_config.DISTINCT_OFFICERS_WITH_TASKS_COMPLETED,
    metric_config.INCARCERATION_LENGTH_OF_STAY_BY_END,
    metric_config.INCARCERATION_ENDS,
    metric_config.INCARCERATION_ENDS_OFFICIAL_RELEASES_NOT_DEATH,
    metric_config.SUM_EARNED_CREDITS,
    metric_config.POSITIVE_EARNED_CREDIT_EVENTS,
    metric_config.PERSON_DAYS_WEIGHTED_EARNED_CREDIT_BALANCE,
    metric_config.INCARCERATION_INCIDENTS,
    metric_config.AVG_DAILY_POPULATION_WITH_PRIOR_INCARCERATION_INCIDENT,
    metric_config.DAYS_SINCE_MOST_RECENT_INCARCERATION_INCIDENT,
    *metric_config.AVG_DAILY_POPULATION_CUSTODY_LEVEL_METRICS,
    metric_config.AVG_DAILY_POPULATION_SOLITARY_CONFINEMENT,
    metric_config.CONTACT_DUE_DATES_MET,
    metric_config.CONTACT_DUE_DATES,
    metric_config.PAROLE_BOARD_HEARINGS_APPROVED,
    metric_config.PAROLE_BOARD_HEARINGS,
]

EXPERIMENT_KPI_DENOMINATOR_METRICS: list[AggregatedMetric] = [
    metric_config.AVG_DAILY_POPULATION,
    metric_config.TASK_COMPLETIONS,
    metric_config.DISTINCT_REGISTERED_PRIMARY_INSIGHTS_USERS,
    metric_config.DISTINCT_REGISTERED_PRIMARY_WORKFLOWS_USERS,
    metric_config.AVG_DAILY_POPULATION_TASK_ELIGIBLE,
    metric_config.DISTINCT_OFFICERS_WITH_ELIGIBLE_CASELOAD,
    metric_config.CONTACT_DUE_DATES,
    metric_config.PAROLE_BOARD_HEARINGS,
]
