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

from recidiviz.observations.metric_unit_of_observation_type import (
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
    CUSTODY_LEVEL_SESSION = "CUSTODY_LEVEL_SESSION"
    EMPLOYMENT_PERIOD = "EMPLOYMENT_PERIOD"
    EMPLOYMENT_STATUS_SESSION = "EMPLOYMENT_STATUS_SESSION"
    GLOBAL_PROVISIONED_USER_SESSION = "GLOBAL_PROVISIONED_USER_SESSION"
    HOUSING_TYPE_SESSION = "HOUSING_TYPE_SESSION"
    HOUSING_UNIT_TYPE_COLLAPSED_SOLITARY_SESSION = (
        "HOUSING_UNIT_TYPE_COLLAPSED_SOLITARY_SESSION"
    )
    INSIGHTS_SUPERVISION_OFFICER_CASELOAD_CATEGORY_SESSION = (
        "INSIGHTS_SUPERVISION_OFFICER_CASELOAD_CATEGORY_SESSION"
    )
    INSIGHTS_SUPERVISION_OFFICER_OUTLIER_STATUS_SESSION = (
        "INSIGHTS_SUPERVISION_OFFICER_OUTLIER_STATUS_SESSION"
    )
    INSIGHTS_PRIMARY_USER_IMPACT_FUNNEL_STATUS_SESSION = (
        "INSIGHTS_PRIMARY_USER_IMPACT_FUNNEL_STATUS_SESSION"
    )
    INSIGHTS_PRIMARY_USER_REGISTRATION_SESSION = (
        "INSIGHTS_PRIMARY_USER_REGISTRATION_SESSION"
    )
    INSIGHTS_PROVISIONED_USER_SESSION = "INSIGHTS_PROVISIONED_USER_SESSION"
    JII_TABLET_APP_PROVISIONED_USER_SESSION = "JII_TABLET_APP_PROVISIONED_USER_SESSION"
    JUSTICE_IMPACT_SESSION = "JUSTICE_IMPACT_SESSION"
    LOCATION_TYPE_SESSION = "LOCATION_TYPE_SESSION"
    PERSON_DEMOGRAPHICS = "PERSON_DEMOGRAPHICS"
    SUPERVISION_ASSESSMENT_COMPLIANCE_SPAN = "SUPERVISION_ASSESSMENT_COMPLIANCE_SPAN"
    SUPERVISION_CONTACT_COMPLIANCE_SPAN = "SUPERVISION_CONTACT_COMPLIANCE_SPAN"
    SUPERVISION_LEVEL_SESSION = "SUPERVISION_LEVEL_SESSION"
    SUPERVISION_OFFICER_CASELOAD_COUNT_SPAN = "SUPERVISION_OFFICER_CASELOAD_COUNT_SPAN"
    SUPERVISION_OFFICER_INFERRED_LOCATION_SESSION = (
        "SUPERVISION_OFFICER_INFERRED_LOCATION_SESSION"
    )
    SUPERVISION_OFFICER_ELIGIBILITY_SESSIONS = (
        "SUPERVISION_OFFICER_ELIGIBILITY_SESSIONS"
    )
    SUPERVISION_OFFICER_SESSION = "SUPERVISION_OFFICER_SESSION"
    TASK_CRITERIA_SPAN = "TASK_CRITERIA_SPAN"
    TASK_ELIGIBILITY_SESSION = "TASK_ELIGIBILITY_SESSION"
    TASKS_PRIMARY_USER_REGISTRATION_SESSION = "TASKS_PRIMARY_USER_REGISTRATION_SESSION"
    TASKS_PROVISIONED_USER_SESSION = "TASKS_PROVISIONED_USER_SESSION"
    US_AR_OVG_SESSIONS = "US_AR_OVG_SESSIONS"
    WORKFLOWS_SURFACEABLE_CASELOAD_SESSION = "WORKFLOWS_SURFACEABLE_CASELOAD_SESSION"
    WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION = (
        "WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION"
    )
    WORKFLOWS_PERSON_OPPORTUNITY_TAB_TYPE_SESSION = (
        "WORKFLOWS_PERSON_OPPORTUNITY_TAB_TYPE_SESSION"
    )
    WORKFLOWS_USER_CASELOAD_ACCESS_SESSION = "WORKFLOWS_USER_CASELOAD_ACCESS_SESSION"
    WORKFLOWS_PRIMARY_USER_REGISTRATION_SESSION = (
        "WORKFLOWS_PRIMARY_USER_REGISTRATION_SESSION"
    )
    WORKFLOWS_PROVISIONED_USER_SESSION = "WORKFLOWS_PROVISIONED_USER_SESSION"

    @classmethod
    def observation_type_category(cls) -> str:
        return "span"

    @property
    def unit_of_observation_type(self) -> MetricUnitOfObservationType:
        """Returns the unit of observation type associated with the span type"""
        if self in [
            SpanType.ASSESSMENT_SCORE_SESSION,
            SpanType.COMPARTMENT_SESSION,
            SpanType.COMPARTMENT_SUB_SESSION,
            SpanType.CUSTODY_LEVEL_SESSION,
            SpanType.EMPLOYMENT_PERIOD,
            SpanType.EMPLOYMENT_STATUS_SESSION,
            SpanType.HOUSING_TYPE_SESSION,
            SpanType.HOUSING_UNIT_TYPE_COLLAPSED_SOLITARY_SESSION,
            SpanType.JUSTICE_IMPACT_SESSION,
            SpanType.LOCATION_TYPE_SESSION,
            SpanType.PERSON_DEMOGRAPHICS,
            SpanType.SUPERVISION_ASSESSMENT_COMPLIANCE_SPAN,
            SpanType.SUPERVISION_CONTACT_COMPLIANCE_SPAN,
            SpanType.SUPERVISION_LEVEL_SESSION,
            SpanType.SUPERVISION_OFFICER_SESSION,
            SpanType.TASK_CRITERIA_SPAN,
            SpanType.TASK_ELIGIBILITY_SESSION,
            SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
            SpanType.WORKFLOWS_PERSON_OPPORTUNITY_TAB_TYPE_SESSION,
            SpanType.US_AR_OVG_SESSIONS,
        ]:
            return MetricUnitOfObservationType.PERSON_ID
        if self in [
            SpanType.SUPERVISION_OFFICER_INFERRED_LOCATION_SESSION,
            SpanType.INSIGHTS_SUPERVISION_OFFICER_CASELOAD_CATEGORY_SESSION,
            SpanType.INSIGHTS_SUPERVISION_OFFICER_OUTLIER_STATUS_SESSION,
            SpanType.SUPERVISION_OFFICER_ELIGIBILITY_SESSIONS,
            SpanType.SUPERVISION_OFFICER_CASELOAD_COUNT_SPAN,
        ]:
            return MetricUnitOfObservationType.SUPERVISION_OFFICER
        if self in [
            SpanType.WORKFLOWS_USER_CASELOAD_ACCESS_SESSION,
            SpanType.WORKFLOWS_PRIMARY_USER_REGISTRATION_SESSION,
        ]:
            return MetricUnitOfObservationType.WORKFLOWS_PRIMARY_USER
        if self in [SpanType.WORKFLOWS_PROVISIONED_USER_SESSION]:
            return MetricUnitOfObservationType.WORKFLOWS_PROVISIONED_USER
        if self in [
            SpanType.WORKFLOWS_SURFACEABLE_CASELOAD_SESSION,
        ]:
            return MetricUnitOfObservationType.WORKFLOWS_SURFACEABLE_CASELOAD
        if self in [
            SpanType.INSIGHTS_PRIMARY_USER_IMPACT_FUNNEL_STATUS_SESSION,
            SpanType.INSIGHTS_PRIMARY_USER_REGISTRATION_SESSION,
        ]:
            return MetricUnitOfObservationType.INSIGHTS_PRIMARY_USER
        if self in [SpanType.INSIGHTS_PROVISIONED_USER_SESSION]:
            return MetricUnitOfObservationType.INSIGHTS_PROVISIONED_USER
        if self in [
            SpanType.TASKS_PRIMARY_USER_REGISTRATION_SESSION,
        ]:
            return MetricUnitOfObservationType.TASKS_PRIMARY_USER
        if self in [SpanType.TASKS_PROVISIONED_USER_SESSION]:
            return MetricUnitOfObservationType.TASKS_PROVISIONED_USER
        if self in [SpanType.GLOBAL_PROVISIONED_USER_SESSION]:
            return MetricUnitOfObservationType.GLOBAL_PROVISIONED_USER
        if self in [SpanType.JII_TABLET_APP_PROVISIONED_USER_SESSION]:
            return MetricUnitOfObservationType.JII_TABLET_APP_PROVISIONED_USER

        raise ValueError(f"No unit_of_observation_type found for SpanType {self.value}")
