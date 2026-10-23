# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Configured metrics for a custom earned credit metrics explore displayable in
Looker."""

import recidiviz.aggregated_metrics.models.aggregated_metric_configurations as metric_config
from recidiviz.aggregated_metrics.models.aggregated_metric import AggregatedMetric
from recidiviz.aggregated_metrics.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)

EARNED_CREDIT_ASSIGNMENT_NAMES_TO_TYPES = {
    "ALL_INCARCERATION_STATES": (
        MetricPopulationType.INCARCERATION,
        MetricUnitOfAnalysisType.ALL_STATES,
    ),
    "INCARCERATION_STATE": (
        MetricPopulationType.INCARCERATION,
        MetricUnitOfAnalysisType.STATE_CODE,
    ),
    "INCARCERATION_FACILITY": (
        MetricPopulationType.INCARCERATION,
        MetricUnitOfAnalysisType.FACILITY,
    ),
    "ALL_SUPERVISION_STATES": (
        MetricPopulationType.SUPERVISION,
        MetricUnitOfAnalysisType.ALL_STATES,
    ),
    "SUPERVISION_STATE": (
        MetricPopulationType.SUPERVISION,
        MetricUnitOfAnalysisType.STATE_CODE,
    ),
    "ALL_JUSTICE_INVOLVED_STATES": (
        MetricPopulationType.JUSTICE_INVOLVED,
        MetricUnitOfAnalysisType.ALL_STATES,
    ),
    "JUSTICE_INVOLVED_STATE": (
        MetricPopulationType.JUSTICE_INVOLVED,
        MetricUnitOfAnalysisType.STATE_CODE,
    ),
}

# Suggestions are the displayed values: the generated dimension applies
# INITCAP(REPLACE(field, '_', ' ')) to each one.
EARNED_CREDIT_JSON_FIELD_FILTERS_WITH_SUGGESTIONS: dict[str, list[str]] = {
    # US_CO
    "pathway": ["Cert", "Edovo", "Ged", "Unrecognized", "Work"],
    "outcome_category": [
        "Applied Successfully",
        "Error Other",
        "In Progress",
        "Refused",
        "Skipped",
        "Uncertain",
        "Wedged",
    ],
    "eomis_status_at_write": [
        "Applied",
        "Ineligible",
        "Multiple",
        "Partially Applied",
        "Pending",
        "Probated",
        "Unrecognized",
        "Void",
    ],
}

EARNED_CREDIT_LOOKER_METRICS: list[AggregatedMetric] = [
    metric_config.AVG_DAILY_POPULATION,
    metric_config.EARNED_CREDIT_WRITEBACK_ATTEMPT_EVENTS,
    metric_config.DISTINCT_POPULATION_EARNED_CREDIT_WRITEBACK_ATTEMPT,
    metric_config.SUM_EARNED_CREDIT_WRITEBACK_DAYS_REQUESTED,
    metric_config.SUM_EARNED_CREDIT_WRITEBACK_DAYS_ATTEMPTED,
    metric_config.SUM_EARNED_CREDIT_WRITEBACK_DAYS_WRITTEN,
    metric_config.SUM_EARNED_CREDIT_WRITEBACK_DAYS_APPLIED,
    metric_config.DISTINCT_POPULATION_WITH_EARNED_CREDIT_WRITEBACK_DAYS_APPLIED,
]
