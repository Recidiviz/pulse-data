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

from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    MetricUnitOfObservationType,
)


class EventType(Enum):
    """Category of event-shaped data"""

    ABSCONSION_BENCH_WARRANT = "ABSCONSION_BENCH_WARRANT"
    COMPARTMENT_LEVEL_2_START = "COMPARTMENT_LEVEL_2_START"
    CUSTODY_LEVEL_CHANGE = "CUSTODY_LEVEL_CHANGE"
    DRUG_SCREEN = "DRUG_SCREEN"
    EARLY_DISCHARGE_REQUEST = "EARLY_DISCHARGE_REQUEST"
    EARLY_DISCHARGE_REQUEST_DECISION = "EARLY_DISCHARGE_REQUEST_DECISION"
    EMPLOYMENT_PERIOD_START = "EMPLOYMENT_PERIOD_START"
    EMPLOYMENT_STATUS_CHANGE = "EMPLOYMENT_STATUS_CHANGE"
    INCARCERATION_INCIDENT = "INCARCERATION_INCIDENT"
    INCARCERATION_START_TEMPORARY = "INCARCERATION_START_TEMPORARY"
    INCARCERATION_START = "INCARCERATION_START"
    INCARCERATION_RELEASE = "INCARCERATION_RELEASE"
    LIBERTY_START = "LIBERTY_START"
    PAROLE_HEARING = "PAROLE_HEARING"
    PENDING_CUSTODY_START = "PENDING_CUSTODY_START"
    RISK_SCORE_ASSESSMENT = "RISK_SCORE_ASSESSMENT"
    SENTENCES_IMPOSED = "SENTENCES_IMPOSED"
    SUPERVISING_OFFICER_CHANGE = "SUPERVISING_OFFICER_CHANGE"
    SUPERVISING_OFFICER_NEW_ASSIGNMENT = "SUPERVISING_OFFICER_NEW_ASSIGNMENT"
    SUPERVISION_CONTACT = "SUPERVISION_CONTACT"
    SUPERVISION_LEVEL_CHANGE = "SUPERVISION_LEVEL_CHANGE"
    SUPERVISION_START = "SUPERVISION_START"
    SUPERVISION_RELEASE = "SUPERVISION_RELEASE"
    SUPERVISION_TERMINATION_WITH_INCARCERATION_REASON = (
        "SUPERVISION_TERMINATION_WITH_INCARCERATION_REASON"
    )
    TASK_COMPLETED = "TASK_COMPLETED"
    TASK_ELIGIBILITY_START = "TASK_ELIGIBILITY_START"
    TASK_ELIGIBLE_30_DAYS = "TASK_ELIGIBLE_30_DAYS"
    TASK_ELIGIBLE_7_DAYS = "TASK_ELIGIBLE_7_DAYS"
    TREATMENT_REFERRAL = "TREATMENT_REFERRAL"
    TREATMENT_START = "TREATMENT_START"
    VARIANT_ASSIGNMENT = "VARIANT_ASSIGNMENT"
    VIOLATION = "VIOLATION"
    VIOLATION_RESPONSE = "VIOLATION_RESPONSE"
    WORKFLOWS_ACTION = "WORKFLOWS_ACTION"
    WORKFLOWS_CLIENT_STATUS_UPDATE = "WORKFLOWS_CLIENT_STATUS_UPDATE"
    WORKFLOWS_PAGE = "WORKFLOWS_PAGE"

    @property
    def unit_of_observation_type(self) -> MetricUnitOfObservationType:
        """Returns the unit of observation type associated with the event type"""
        if self in [
            EventType.ABSCONSION_BENCH_WARRANT.ABSCONSION_BENCH_WARRANT,
            EventType.COMPARTMENT_LEVEL_2_START,
            EventType.CUSTODY_LEVEL_CHANGE,
            EventType.DRUG_SCREEN,
            EventType.EARLY_DISCHARGE_REQUEST,
            EventType.EARLY_DISCHARGE_REQUEST_DECISION,
            EventType.EMPLOYMENT_PERIOD_START,
            EventType.EMPLOYMENT_STATUS_CHANGE,
            EventType.INCARCERATION_INCIDENT,
            EventType.INCARCERATION_START_TEMPORARY,
            EventType.INCARCERATION_START,
            EventType.INCARCERATION_RELEASE,
            EventType.LIBERTY_START,
            EventType.PAROLE_HEARING,
            EventType.PENDING_CUSTODY_START,
            EventType.RISK_SCORE_ASSESSMENT,
            EventType.SENTENCES_IMPOSED,
            EventType.SUPERVISING_OFFICER_CHANGE,
            EventType.SUPERVISING_OFFICER_NEW_ASSIGNMENT,
            EventType.SUPERVISION_CONTACT,
            EventType.SUPERVISION_LEVEL_CHANGE,
            EventType.SUPERVISION_START,
            EventType.SUPERVISION_RELEASE,
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
        ]:
            return MetricUnitOfObservationType.PERSON_ID
        if self in [
            EventType.WORKFLOWS_ACTION,
            EventType.WORKFLOWS_CLIENT_STATUS_UPDATE,
            EventType.WORKFLOWS_PAGE,
        ]:
            return MetricUnitOfObservationType.SUPERVISION_OFFICER

        raise ValueError(
            f"No unit_of_observation_type found for EventType {self.value}"
        )
