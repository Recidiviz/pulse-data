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
"""Defines EventType enum."""
from enum import Enum

from recidiviz.observations.metric_unit_of_observation_type import (
    MetricUnitOfObservationType,
)


class EventType(Enum):
    """Category of event-shaped data"""

    ABSCONSION_BENCH_WARRANT = "ABSCONSION_BENCH_WARRANT"
    ALL_TOOLS_LINE_STAFF_ACTIVE_USAGE_EVENT = "ALL_TOOLS_LINE_STAFF_ACTIVE_USAGE_EVENT"
    COMPARTMENT_LEVEL_2_START = "COMPARTMENT_LEVEL_2_START"
    CUSTODY_LEVEL_CHANGE = "CUSTODY_LEVEL_CHANGE"
    DRUG_SCREEN = "DRUG_SCREEN"
    EARLY_DISCHARGE_REQUEST = "EARLY_DISCHARGE_REQUEST"
    EARLY_DISCHARGE_REQUEST_DECISION = "EARLY_DISCHARGE_REQUEST_DECISION"
    EMPLOYMENT_PERIOD_START = "EMPLOYMENT_PERIOD_START"
    EMPLOYMENT_STATUS_CHANGE = "EMPLOYMENT_STATUS_CHANGE"
    GLOBAL_USER_ACTIVE_USAGE_EVENT = "GLOBAL_USER_ACTIVE_USAGE_EVENT"
    GLOBAL_USER_LOGIN = "GLOBAL_USER_LOGIN"
    GLOBAL_USER_PAGE_VIEW = "GLOBAL_USER_PAGE_VIEW"
    HOUSING_UNIT_TYPE_END = "HOUSING_UNIT_TYPE_END"
    HOUSING_UNIT_TYPE_START = "HOUSING_UNIT_TYPE_START"
    INCARCERATION_INCIDENT = "INCARCERATION_INCIDENT"
    INCARCERATION_START_TEMPORARY = "INCARCERATION_START_TEMPORARY"
    INCARCERATION_START = "INCARCERATION_START"
    INCARCERATION_START_AND_INFERRED_START = "INCARCERATION_START_AND_INFERRED_START"
    INSIGHTS_ACTIVE_USAGE_EVENT = "INSIGHTS_ACTIVE_USAGE_EVENT"
    INSIGHTS_USER_LOGIN = "INSIGHTS_USER_LOGIN"
    IMPACT_TRANSITION = "IMPACT_TRANSITION"
    INTERCOM_CSAT_RESPONSE = "INTERCOM_CSAT_RESPONSE"
    JII_TABLET_APP_USER_LOGIN = "JII_TABLET_APP_USER_LOGIN"
    JII_OUTGOING_TEXT_MESSAGE = "JII_OUTGOING_TEXT_MESSAGE"
    PAROLE_HEARING = "PAROLE_HEARING"
    PENDING_CUSTODY_START = "PENDING_CUSTODY_START"
    RISK_SCORE_ASSESSMENT = "RISK_SCORE_ASSESSMENT"
    SENTENCES_IMPOSED = "SENTENCES_IMPOSED"
    SOLITARY_CONFINEMENT_END = "SOLITARY_CONFINEMENT_END"
    SOLITARY_CONFINEMENT_START = "SOLITARY_CONFINEMENT_START"
    SUPERVISING_OFFICER_CHANGE = "SUPERVISING_OFFICER_CHANGE"
    SUPERVISION_CONTACT = "SUPERVISION_CONTACT"
    SUPERVISION_CONTACT_DUE = "SUPERVISION_CONTACT_DUE"
    SUPERVISION_LEVEL_CHANGE = "SUPERVISION_LEVEL_CHANGE"
    SUPERVISION_OFFICER_TASK_COMPLETED = "SUPERVISION_OFFICER_TASK_COMPLETED"
    SUPERVISION_START = "SUPERVISION_START"
    SUPERVISION_TERMINATION_WITH_INCARCERATION_REASON = (
        "SUPERVISION_TERMINATION_WITH_INCARCERATION_REASON"
    )
    TASK_COMPLETED = "TASK_COMPLETED"
    TASK_ELIGIBILITY_START = "TASK_ELIGIBILITY_START"
    TASK_ELIGIBLE_30_DAYS = "TASK_ELIGIBLE_30_DAYS"
    TASK_ELIGIBLE_7_DAYS = "TASK_ELIGIBLE_7_DAYS"
    TASKS_ACTIVE_USAGE_EVENT = "TASKS_ACTIVE_USAGE_EVENT"
    TASKS_USER_LOGIN = "TASKS_USER_LOGIN"
    TRANSITIONS_TO_LIBERTY_ALL = "TRANSITIONS_TO_LIBERTY_ALL"
    TREATMENT_REFERRAL = "TREATMENT_REFERRAL"
    TREATMENT_START = "TREATMENT_START"
    VARIANT_ASSIGNMENT = "VARIANT_ASSIGNMENT"
    VIOLATION = "VIOLATION"
    VIOLATION_RESPONSE = "VIOLATION_RESPONSE"
    WORKFLOWS_CASELOAD_SURFACED = "WORKFLOWS_CASELOAD_SURFACED"
    WORKFLOWS_PERSON_USAGE_EVENT = "WORKFLOWS_PERSON_USAGE_EVENT"

    # The following workflows user enums capture types of usage events in the
    # Workflows tool. Any additional event types should be added to any downstream
    # usage-related metrics or views that reference workflows_user_events.
    # e.g., `analyst_data.workflows_live_completion_event_types_by_state`

    # Event tracking all activity that qualifies a Workflows user as "active"
    WORKFLOWS_ACTIVE_USAGE_EVENT = "WORKFLOWS_ACTIVE_USAGE_EVENT"
    WORKFLOWS_USER_LOGIN = "WORKFLOWS_USER_LOGIN"
    # Event where the user triggered a new snooze status
    WORKFLOWS_USER_SNOOZE_ACTION = "WORKFLOWS_USER_SNOOZE_ACTION"

    US_AR_OVG_TRANCHE_CHANGES = "US_AR_OVG_TRANCHE_CHANGES"
    US_AR_INCENTIVES = "US_AR_INCENTIVES"

    @classmethod
    def observation_type_category(cls) -> str:
        return "event"

    @property
    def unit_of_observation_type(self) -> MetricUnitOfObservationType:
        """Returns the unit of observation type associated with the event type"""
        if self in [
            EventType.ABSCONSION_BENCH_WARRANT,
            EventType.COMPARTMENT_LEVEL_2_START,
            EventType.CUSTODY_LEVEL_CHANGE,
            EventType.DRUG_SCREEN,
            EventType.EARLY_DISCHARGE_REQUEST,
            EventType.EARLY_DISCHARGE_REQUEST_DECISION,
            EventType.EMPLOYMENT_PERIOD_START,
            EventType.EMPLOYMENT_STATUS_CHANGE,
            EventType.HOUSING_UNIT_TYPE_END,
            EventType.HOUSING_UNIT_TYPE_START,
            EventType.INCARCERATION_INCIDENT,
            EventType.INCARCERATION_START_TEMPORARY,
            EventType.INCARCERATION_START,
            EventType.INCARCERATION_START_AND_INFERRED_START,
            EventType.TRANSITIONS_TO_LIBERTY_ALL,
            EventType.IMPACT_TRANSITION,
            EventType.JII_OUTGOING_TEXT_MESSAGE,
            EventType.PAROLE_HEARING,
            EventType.PENDING_CUSTODY_START,
            EventType.RISK_SCORE_ASSESSMENT,
            EventType.SENTENCES_IMPOSED,
            EventType.SOLITARY_CONFINEMENT_END,
            EventType.SOLITARY_CONFINEMENT_START,
            EventType.SUPERVISING_OFFICER_CHANGE,
            EventType.SUPERVISION_CONTACT,
            EventType.SUPERVISION_CONTACT_DUE,
            EventType.SUPERVISION_LEVEL_CHANGE,
            EventType.SUPERVISION_START,
            EventType.SUPERVISION_TERMINATION_WITH_INCARCERATION_REASON,
            EventType.TASK_COMPLETED,
            EventType.TASK_ELIGIBILITY_START,
            EventType.TASK_ELIGIBLE_30_DAYS,
            EventType.TASK_ELIGIBLE_7_DAYS,
            EventType.TREATMENT_REFERRAL,
            EventType.TREATMENT_START,
            EventType.VARIANT_ASSIGNMENT,
            EventType.VIOLATION,
            EventType.VIOLATION_RESPONSE,
            EventType.WORKFLOWS_PERSON_USAGE_EVENT,
            EventType.US_AR_INCENTIVES,
            EventType.US_AR_OVG_TRANCHE_CHANGES,
        ]:
            return MetricUnitOfObservationType.PERSON_ID
        if self in [EventType.SUPERVISION_OFFICER_TASK_COMPLETED]:
            return MetricUnitOfObservationType.SUPERVISION_OFFICER
        if self in [
            EventType.WORKFLOWS_ACTIVE_USAGE_EVENT,
            EventType.WORKFLOWS_USER_LOGIN,
            EventType.WORKFLOWS_USER_SNOOZE_ACTION,
            EventType.ALL_TOOLS_LINE_STAFF_ACTIVE_USAGE_EVENT,
        ]:
            return MetricUnitOfObservationType.WORKFLOWS_PRIMARY_USER
        if self in [
            EventType.INSIGHTS_USER_LOGIN,
            EventType.INSIGHTS_ACTIVE_USAGE_EVENT,
        ]:
            return MetricUnitOfObservationType.INSIGHTS_PRIMARY_USER
        if self in [EventType.WORKFLOWS_CASELOAD_SURFACED]:
            return MetricUnitOfObservationType.WORKFLOWS_SURFACEABLE_CASELOAD
        if self in [
            EventType.TASKS_USER_LOGIN,
            EventType.TASKS_ACTIVE_USAGE_EVENT,
        ]:
            return MetricUnitOfObservationType.TASKS_PRIMARY_USER
        if self in [
            EventType.GLOBAL_USER_LOGIN,
            EventType.GLOBAL_USER_ACTIVE_USAGE_EVENT,
            EventType.GLOBAL_USER_PAGE_VIEW,
            EventType.INTERCOM_CSAT_RESPONSE,
        ]:
            return MetricUnitOfObservationType.GLOBAL_PROVISIONED_USER
        if self in [EventType.JII_TABLET_APP_USER_LOGIN]:
            return MetricUnitOfObservationType.JII_TABLET_APP_PROVISIONED_USER

        raise ValueError(
            f"No unit_of_observation_type found for EventType {self.value}"
        )
