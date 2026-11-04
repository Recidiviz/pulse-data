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

from recidiviz.aggregated_metrics.configuration.collections.standard import (
    METRICS_BY_POPULATION_TYPE,
)
from recidiviz.aggregated_metrics.models.aggregated_metric import AggregatedMetric
from recidiviz.aggregated_metrics.models.aggregated_metric_configurations import (
    APPROVED_RISK_ASSESSMENT_OVERRIDES,
    APPROVED_RISK_ASSESSMENTS,
)
from recidiviz.aggregated_metrics.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentLevel,
    StateAssessmentSubtype,
    StateAssessmentType,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodCustodyLevel,
)
from recidiviz.common.str_field_utils import snake_to_title

# Risk assessment metrics live only here, not in the standard collection, to keep
# the materialized aggregated_metrics views under their time limits.
INCARCERATION_SYSTEM_HEALTH_LOOKER_METRICS: list[AggregatedMetric] = [
    *METRICS_BY_POPULATION_TYPE[MetricPopulationType.INCARCERATION],
    APPROVED_RISK_ASSESSMENTS,
    APPROVED_RISK_ASSESSMENT_OVERRIDES,
]

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

_CUSTODY_LEVEL_SUGGESTIONS = [
    snake_to_title(level.value) for level in StateIncarcerationPeriodCustodyLevel
]

_ASSESSMENT_LEVEL_SUGGESTIONS = [
    snake_to_title(level.value) for level in StateAssessmentLevel
]

_ASSESSMENT_TYPE_SUGGESTIONS = [
    snake_to_title(assessment_type.value) for assessment_type in StateAssessmentType
]

_ASSESSMENT_SUBTYPE_SUGGESTIONS = [
    snake_to_title(subtype.value) for subtype in StateAssessmentSubtype
]

INCARCERATION_SYSTEM_HEALTH_JSON_FIELD_FILTERS_WITH_SUGGESTIONS: dict[
    str, list[str]
] = {
    "previous_custody_level": _CUSTODY_LEVEL_SUGGESTIONS,
    "new_custody_level": _CUSTODY_LEVEL_SUGGESTIONS,
    "assessment_level": _ASSESSMENT_LEVEL_SUGGESTIONS,
    "assessment_override_level": _ASSESSMENT_LEVEL_SUGGESTIONS,
    "assessment_result_level": _ASSESSMENT_LEVEL_SUGGESTIONS,
    "assessment_type": _ASSESSMENT_TYPE_SUGGESTIONS,
    "assessment_subtype": _ASSESSMENT_SUBTYPE_SUGGESTIONS,
    "assessment_score": [],
}
