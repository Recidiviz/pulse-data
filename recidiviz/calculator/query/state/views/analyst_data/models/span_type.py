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
"""Defines SpanType enum."""

from enum import Enum

from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    MetricUnitOfObservationType,
)


class SpanType(Enum):
    """Enum identifying the source of span-shaped data. Corresponds to a single
    SpanQueryBuilder entry in
    recidiviz/calculator/query/state/views/analyst_data/models/spans.py.
    """

    ASSESSMENT_SCORE_SESSION = "ASSESSMENT_SCORE_SESSION"
    COMPARTMENT_SESSION = "COMPARTMENT_SESSION"
    COMPARTMENT_SUB_SESSION = "COMPARTMENT_SUB_SESSION"
    COMPLETED_CONTACT_SESSION = "COMPLETED_CONTACT_SESSION"
    CUSTODY_LEVEL_SESSION = "CUSTODY_LEVEL_SESSION"
    EMPLOYMENT_PERIOD = "EMPLOYMENT_PERIOD"
    EMPLOYMENT_STATUS_SESSION = "EMPLOYMENT_STATUS_SESSION"
    HOUSING_TYPE_SESSION = "HOUSING_TYPE_SESSION"
    JUSTICE_IMPACT_SESSION = "JUSTICE_IMPACT_SESSION"
    PERSON_DEMOGRAPHICS = "PERSON_DEMOGRAPHICS"
    SENTENCE_SPAN = "SENTENCE_SPAN"
    SUPERVISION_LEVEL_DOWNGRADE_ELIGIBLE = "SUPERVISION_LEVEL_DOWNGRADE_ELIGIBLE"
    SUPERVISION_LEVEL_SESSION = "SUPERVISION_LEVEL_SESSION"
    SUPERVISION_OFFICER_INFERRED_LOCATION_SESSION = (
        "SUPERVISION_OFFICER_INFERRED_LOCATION_SESSION"
    )
    SUPERVISION_OFFICER_SESSION = "SUPERVISION_OFFICER_SESSION"
    TASK_CRITERIA_SPAN = "TASK_CRITERIA_SPAN"
    TASK_ELIGIBILITY_SESSION = "TASK_ELIGIBILITY_SESSION"
    WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION = (
        "WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION"
    )
    WORKFLOWS_USER_CASELOAD_ACCESS_SESSION = "WORKFLOWS_USER_CASELOAD_ACCESS_SESSION"

    @property
    def unit_of_observation_type(self) -> MetricUnitOfObservationType:
        """Returns the unit of observation type associated with the span type"""
        if self in [
            SpanType.ASSESSMENT_SCORE_SESSION,
            SpanType.COMPARTMENT_SESSION,
            SpanType.COMPARTMENT_SUB_SESSION,
            SpanType.COMPLETED_CONTACT_SESSION,
            SpanType.CUSTODY_LEVEL_SESSION,
            SpanType.EMPLOYMENT_PERIOD,
            SpanType.EMPLOYMENT_STATUS_SESSION,
            SpanType.HOUSING_TYPE_SESSION,
            SpanType.JUSTICE_IMPACT_SESSION,
            SpanType.PERSON_DEMOGRAPHICS,
            SpanType.SENTENCE_SPAN,
            SpanType.SUPERVISION_LEVEL_DOWNGRADE_ELIGIBLE,
            SpanType.SUPERVISION_LEVEL_SESSION,
            SpanType.SUPERVISION_OFFICER_SESSION,
            SpanType.TASK_CRITERIA_SPAN,
            SpanType.TASK_ELIGIBILITY_SESSION,
            SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
        ]:
            return MetricUnitOfObservationType.PERSON_ID
        if self in [SpanType.SUPERVISION_OFFICER_INFERRED_LOCATION_SESSION]:
            return MetricUnitOfObservationType.SUPERVISION_OFFICER
        if self in [SpanType.WORKFLOWS_USER_CASELOAD_ACCESS_SESSION]:
            return MetricUnitOfObservationType.WORKFLOWS_USER

        raise ValueError(f"No unit_of_observation_type found for SpanType {self.value}")
