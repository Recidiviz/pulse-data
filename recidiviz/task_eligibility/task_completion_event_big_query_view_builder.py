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
"""Defines a BigQueryViewBuilder that can be used to define task completion events of a
given type. These views are used as inputs to a task eligibility spans view.
"""
from enum import Enum
from typing import List, Optional, Union

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.common.constants.states import StateCode
from recidiviz.common.decarceral_impact_type import DecarceralImpactType
from recidiviz.task_eligibility.dataset_config import (
    TASK_COMPLETION_EVENTS_DATASET_ID,
    completion_event_state_specific_dataset,
)
from recidiviz.workflows.types import WorkflowsSystemType


# TODO(#46985): Rename this object and move it to a Workflows specific module
class TaskCompletionEventType(Enum):
    """Enum describing the type of task completion event"""

    # TODO(#35495): Rename these to `FULL_TERM_DISCHARGE_FROM_SUPERVISION` and `EARLY_DISCHARGE_FROM_SUPERVISION`
    FULL_TERM_DISCHARGE = "FULL_TERM_DISCHARGE"
    EARLY_DISCHARGE = "EARLY_DISCHARGE"
    EARLY_RELEASE_TO_COMMUNITY_CONFINEMENT_SUPERVISION_NOT_OVERDUE = (
        "EARLY_RELEASE_TO_COMMUNITY_CONFINEMENT_SUPERVISION_NOT_OVERDUE"
    )
    EARLY_RELEASE_TO_COMMUNITY_CONFINEMENT_SUPERVISION_OVERDUE = (
        "EARLY_RELEASE_TO_COMMUNITY_CONFINEMENT_SUPERVISION_OVERDUE"
    )
    EARLY_RELEASE_TO_DRUG_PROGRAM_NOT_OVERDUE = (
        "EARLY_RELEASE_TO_DRUG_PROGRAM_NOT_OVERDUE"
    )
    EARLY_RELEASE_TO_DRUG_PROGRAM_OVERDUE = "EARLY_RELEASE_TO_DRUG_PROGRAM_OVERDUE"
    TRANSFER_TO_LIMITED_SUPERVISION = "TRANSFER_TO_LIMITED_SUPERVISION"
    # Subset of TRANSFER_TO_LIMITED_SUPERVISION downgrades starting from MINIMUM supervision
    TRANSFER_TO_LIMITED_SUPERVISION_FROM_MINIMUM = (
        "TRANSFER_TO_LIMITED_SUPERVISION_FROM_MINIMUM"
    )
    TRANSFER_TO_UNSUPERVISED_PAROLE = "TRANSFER_TO_UNSUPERVISED_PAROLE"
    RELEASE_TO_LIMITED_SUPERVISION = "RELEASE_TO_LIMITED_SUPERVISION"
    SUPERVISION_LEVEL_DOWNGRADE = "SUPERVISION_LEVEL_DOWNGRADE"
    CUSTODY_LEVEL_DOWNGRADE = "CUSTODY_LEVEL_DOWNGRADE"
    CUSTODY_LEVEL_DOWNGRADE_TO_MEDIUM_TRUSTEE = (
        "CUSTODY_LEVEL_DOWNGRADE_TO_MEDIUM_TRUSTEE"
    )
    CUSTODY_LEVEL_UPGRADE = "CUSTODY_LEVEL_UPGRADE"
    SECURITY_CLASSIFICATION_COMMITTEE_REVIEW = (
        "SECURITY_CLASSIFICATION_COMMITTEE_REVIEW"
    )
    ADD_IN_PERSON_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW = (
        "ADD_IN_PERSON_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW"
    )
    WARDEN_IN_PERSON_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW = (
        "WARDEN_IN_PERSON_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW"
    )
    SUPERVISION_LEVEL_DOWNGRADE_BEFORE_INITIAL_CLASSIFICATION_REVIEW_DATE = (
        "SUPERVISION_LEVEL_DOWNGRADE_BEFORE_INITIAL_CLASSIFICATION_REVIEW_DATE"
    )
    SUPERVISION_LEVEL_DOWNGRADE_AFTER_INITIAL_CLASSIFICATION_REVIEW_DATE = (
        "SUPERVISION_LEVEL_DOWNGRADE_AFTER_INITIAL_CLASSIFICATION_REVIEW_DATE"
    )
    RELEASE_TO_COMMUNITY_CONFINEMENT_SUPERVISION = (
        "RELEASE_TO_COMMUNITY_CONFINEMENT_SUPERVISION"
    )
    RELEASE_TO_PAROLE = "RELEASE_TO_PAROLE"
    TRANSFER_OUT_OF_SOLITARY_CONFINEMENT = "TRANSFER_OUT_OF_SOLITARY_CONFINEMENT"
    TRANSFER_OUT_OF_ADMINISTRATIVE_SOLITARY_CONFINEMENT = (
        "TRANSFER_OUT_OF_ADMINISTRATIVE_SOLITARY_CONFINEMENT"
    )
    TRANSFER_OUT_OF_DISCIPLINARY_OR_TEMPORARY_SOLITARY_CONFINEMENT = (
        "TRANSFER_OUT_OF_DISCIPLINARY_OR_TEMPORARY_SOLITARY_CONFINEMENT"
    )
    # TODO(#35491): rename these hearing events to be more specific
    HEARING_OCCURRED = "HEARING_OCCURRED"
    REVIEW_HEARING_OCCURRED = "REVIEW_HEARING_OCCURRED"
    # Work release allows incarcerated residents to leave the facility for a
    # period of time (often during the day) to work a job within the community
    GRANTED_WORK_RELEASE = "GRANTED_WORK_RELEASE"
    # Institutional worker status allows incarcerated residents to work within
    # a facility, or work in a very limited capacity outside a facility while
    # monitored by a warden, sheriff, or other officer. In some states (e.g., MO), only
    # assignments outside the facility are included in this completion event; we may
    # need to make adjustments in the future if it becomes necessary to distinguish
    # between within-facility and outside-the-facility assignments within a state.
    GRANTED_INSTITUTIONAL_WORKER_STATUS = "GRANTED_INSTITUTIONAL_WORKER_STATUS"
    GRANTED_FURLOUGH = "GRANTED_FURLOUGH"
    GRANTED_SUPERVISION_SENTENCE_REDUCTION = "GRANTED_SUPERVISION_SENTENCE_REDUCTION"
    # TODO(#35580): determine if this should be split into separate events
    # (potentially transfer to minimum facility & transfer to re-entry facility)
    TRANSFER_TO_MINIMUM_FACILITY = "TRANSFER_TO_MINIMUM_FACILITY"
    INCARCERATION_ASSESSMENT_COMPLETED = "INCARCERATION_ASSESSMENT_COMPLETED"
    INCARCERATION_INTAKE_ASSESSMENT_COMPLETED = (
        "INCARCERATION_INTAKE_ASSESSMENT_COMPLETED"
    )
    KUDOS_SMS_SENT = "KUDOS_SMS_SENT"
    TRANSFER_TO_SPECIAL_CIRCUMSTANCES_SUPERVISION = (
        "TRANSFER_TO_SPECIAL_CIRCUMSTANCES_SUPERVISION"
    )
    TRANSFER_TO_NO_CONTACT_PAROLE = "TRANSFER_TO_NO_CONTACT_PAROLE"
    TRANSFER_TO_SUPERVISION_RUN_FACILITY = "TRANSFER_TO_SUPERVISION_RUN_FACILITY"
    TRANSFER_TO_ADMINISTRATIVE_SUPERVISION = "TRANSFER_TO_ADMINISTRATIVE_SUPERVISION"
    # TODO(#40868): Deprecate this completion event in favor of combining all CR
    # transfers into a single completion event in TN.
    TRANSFER_TO_LIMITED_SUPERVISION_2025_POLICY = (
        "TRANSFER_TO_LIMITED_SUPERVISION_2025_POLICY"
    )
    OVERRIDE_TO_CONDITIONAL_LOW_RISK_SUPERVISION = (
        "OVERRIDE_TO_CONDITIONAL_LOW_RISK_SUPERVISION"
    )
    OVERRIDE_TO_LOW_SUPERVISION = "OVERRIDE_TO_LOW_SUPERVISION"
    SUPERVISION_LEVEL_DOWNGRADE_FROM_MEDIUM_OR_MINIMUM = (
        "SUPERVISION_LEVEL_DOWNGRADE_FROM_MEDIUM_OR_MINIMUM"
    )
    GOOD_TIME_REINSTATED = "GOOD_TIME_REINSTATED"
    FACE_TO_FACE_CONTACT = "FACE_TO_FACE_CONTACT"

    @property
    def system_type(self) -> WorkflowsSystemType:
        """The system type (e.g., FACILITIES vs. SUPERVISION) associated with a completion event type"""
        if self in [
            TaskCompletionEventType.ADD_IN_PERSON_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW,
            TaskCompletionEventType.CUSTODY_LEVEL_DOWNGRADE,
            TaskCompletionEventType.CUSTODY_LEVEL_DOWNGRADE_TO_MEDIUM_TRUSTEE,
            TaskCompletionEventType.CUSTODY_LEVEL_UPGRADE,
            TaskCompletionEventType.EARLY_RELEASE_TO_COMMUNITY_CONFINEMENT_SUPERVISION_NOT_OVERDUE,
            TaskCompletionEventType.EARLY_RELEASE_TO_COMMUNITY_CONFINEMENT_SUPERVISION_OVERDUE,
            TaskCompletionEventType.EARLY_RELEASE_TO_DRUG_PROGRAM_NOT_OVERDUE,
            TaskCompletionEventType.EARLY_RELEASE_TO_DRUG_PROGRAM_OVERDUE,
            TaskCompletionEventType.GRANTED_FURLOUGH,
            TaskCompletionEventType.GRANTED_INSTITUTIONAL_WORKER_STATUS,
            TaskCompletionEventType.GRANTED_WORK_RELEASE,
            TaskCompletionEventType.HEARING_OCCURRED,
            TaskCompletionEventType.INCARCERATION_INTAKE_ASSESSMENT_COMPLETED,
            TaskCompletionEventType.INCARCERATION_ASSESSMENT_COMPLETED,
            TaskCompletionEventType.GOOD_TIME_REINSTATED,
            TaskCompletionEventType.RELEASE_TO_COMMUNITY_CONFINEMENT_SUPERVISION,
            TaskCompletionEventType.RELEASE_TO_LIMITED_SUPERVISION,
            TaskCompletionEventType.RELEASE_TO_PAROLE,
            TaskCompletionEventType.REVIEW_HEARING_OCCURRED,
            TaskCompletionEventType.SECURITY_CLASSIFICATION_COMMITTEE_REVIEW,
            TaskCompletionEventType.TRANSFER_OUT_OF_ADMINISTRATIVE_SOLITARY_CONFINEMENT,
            TaskCompletionEventType.TRANSFER_OUT_OF_DISCIPLINARY_OR_TEMPORARY_SOLITARY_CONFINEMENT,
            TaskCompletionEventType.TRANSFER_OUT_OF_SOLITARY_CONFINEMENT,
            TaskCompletionEventType.TRANSFER_TO_MINIMUM_FACILITY,
            TaskCompletionEventType.TRANSFER_TO_SUPERVISION_RUN_FACILITY,
            TaskCompletionEventType.WARDEN_IN_PERSON_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW,
        ]:
            return WorkflowsSystemType.INCARCERATION
        if self in [
            TaskCompletionEventType.EARLY_DISCHARGE,
            TaskCompletionEventType.FULL_TERM_DISCHARGE,
            TaskCompletionEventType.KUDOS_SMS_SENT,
            TaskCompletionEventType.OVERRIDE_TO_CONDITIONAL_LOW_RISK_SUPERVISION,
            TaskCompletionEventType.OVERRIDE_TO_LOW_SUPERVISION,
            TaskCompletionEventType.SUPERVISION_LEVEL_DOWNGRADE,
            TaskCompletionEventType.SUPERVISION_LEVEL_DOWNGRADE_AFTER_INITIAL_CLASSIFICATION_REVIEW_DATE,
            TaskCompletionEventType.SUPERVISION_LEVEL_DOWNGRADE_BEFORE_INITIAL_CLASSIFICATION_REVIEW_DATE,
            TaskCompletionEventType.TRANSFER_TO_LIMITED_SUPERVISION,
            TaskCompletionEventType.TRANSFER_TO_LIMITED_SUPERVISION_2025_POLICY,
            TaskCompletionEventType.TRANSFER_TO_LIMITED_SUPERVISION_FROM_MINIMUM,
            TaskCompletionEventType.TRANSFER_TO_UNSUPERVISED_PAROLE,
            TaskCompletionEventType.TRANSFER_TO_NO_CONTACT_PAROLE,
            TaskCompletionEventType.TRANSFER_TO_SPECIAL_CIRCUMSTANCES_SUPERVISION,
            TaskCompletionEventType.TRANSFER_TO_ADMINISTRATIVE_SUPERVISION,
            TaskCompletionEventType.SUPERVISION_LEVEL_DOWNGRADE_FROM_MEDIUM_OR_MINIMUM,
            TaskCompletionEventType.GRANTED_SUPERVISION_SENTENCE_REDUCTION,
            TaskCompletionEventType.FACE_TO_FACE_CONTACT,
        ]:
            return WorkflowsSystemType.SUPERVISION
        raise ValueError(
            f"No system type configured for completion event type {self.value}"
        )

    @property
    def decarceral_impact_type(self) -> DecarceralImpactType:
        """Represents the overarching type of decarceral impact (defined as a meaningful movement of
        a justice-impacted individual toward greater liberty) that a given event is meant to
        facilitate, based on the theory of change of Recidiviz tools. Even if a given event does not
        represent the final movement experienced by a JII, the theory of change underlying our tool’s
        influence on this event should ladder up to this impact type. For example, a review hearing
        for release from restrictive housing has an impact type of “release from restrictive housing”,
        because even if the hearing itself does not represent a decarceral shift in the JII’s
        experience in the system, we believe that Recidiviz tools should facilitate hearing events as
        a means for increasing eventual releases from restrictive housing."""
        if self in [
            TaskCompletionEventType.ADD_IN_PERSON_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW,
            TaskCompletionEventType.HEARING_OCCURRED,
            TaskCompletionEventType.REVIEW_HEARING_OCCURRED,
            TaskCompletionEventType.SECURITY_CLASSIFICATION_COMMITTEE_REVIEW,
            TaskCompletionEventType.TRANSFER_OUT_OF_ADMINISTRATIVE_SOLITARY_CONFINEMENT,
            TaskCompletionEventType.TRANSFER_OUT_OF_DISCIPLINARY_OR_TEMPORARY_SOLITARY_CONFINEMENT,
            TaskCompletionEventType.TRANSFER_OUT_OF_SOLITARY_CONFINEMENT,
            TaskCompletionEventType.WARDEN_IN_PERSON_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW,
        ]:
            return DecarceralImpactType.TRANSFER_OUT_OF_SOLITARY_CONFINEMENT
        if self in [
            TaskCompletionEventType.CUSTODY_LEVEL_DOWNGRADE,
            TaskCompletionEventType.CUSTODY_LEVEL_DOWNGRADE_TO_MEDIUM_TRUSTEE,
            TaskCompletionEventType.INCARCERATION_INTAKE_ASSESSMENT_COMPLETED,
            TaskCompletionEventType.INCARCERATION_ASSESSMENT_COMPLETED,
        ]:
            return DecarceralImpactType.DOWNGRADE_CUSTODY_LEVEL
        if self in [
            TaskCompletionEventType.CUSTODY_LEVEL_UPGRADE,
            TaskCompletionEventType.FACE_TO_FACE_CONTACT,
        ]:
            return DecarceralImpactType.NO_DECARCERAL_IMPACT
        if self in [
            TaskCompletionEventType.GRANTED_FURLOUGH,
        ]:
            return DecarceralImpactType.FURLOUGH
        if self in [
            TaskCompletionEventType.GRANTED_INSTITUTIONAL_WORKER_STATUS,
            TaskCompletionEventType.GRANTED_WORK_RELEASE,
        ]:
            return DecarceralImpactType.TRANSFER_TO_WORK_RELEASE
        if self in [
            TaskCompletionEventType.EARLY_RELEASE_TO_COMMUNITY_CONFINEMENT_SUPERVISION_NOT_OVERDUE,
            TaskCompletionEventType.EARLY_RELEASE_TO_COMMUNITY_CONFINEMENT_SUPERVISION_OVERDUE,
            TaskCompletionEventType.EARLY_RELEASE_TO_DRUG_PROGRAM_NOT_OVERDUE,
            TaskCompletionEventType.EARLY_RELEASE_TO_DRUG_PROGRAM_OVERDUE,
            TaskCompletionEventType.RELEASE_TO_COMMUNITY_CONFINEMENT_SUPERVISION,
        ]:
            return DecarceralImpactType.TRANSFER_TO_COMMUNITY_CONFINEMENT
        if self in [
            TaskCompletionEventType.GOOD_TIME_REINSTATED,
            TaskCompletionEventType.RELEASE_TO_PAROLE,
        ]:
            return DecarceralImpactType.RELEASE_TO_PAROLE
        if self in [
            TaskCompletionEventType.TRANSFER_TO_MINIMUM_FACILITY,
            TaskCompletionEventType.TRANSFER_TO_SUPERVISION_RUN_FACILITY,
        ]:
            return DecarceralImpactType.TRANSFER_TO_REENTRY_PREP_UNIT
        if self in [
            TaskCompletionEventType.EARLY_DISCHARGE,
            TaskCompletionEventType.FULL_TERM_DISCHARGE,
        ]:
            return DecarceralImpactType.RELEASE_TO_LIBERTY_FROM_SUPERVISION
        if self in [
            TaskCompletionEventType.KUDOS_SMS_SENT,
            TaskCompletionEventType.OVERRIDE_TO_LOW_SUPERVISION,
            TaskCompletionEventType.SUPERVISION_LEVEL_DOWNGRADE,
            TaskCompletionEventType.SUPERVISION_LEVEL_DOWNGRADE_AFTER_INITIAL_CLASSIFICATION_REVIEW_DATE,
            TaskCompletionEventType.SUPERVISION_LEVEL_DOWNGRADE_BEFORE_INITIAL_CLASSIFICATION_REVIEW_DATE,
            TaskCompletionEventType.SUPERVISION_LEVEL_DOWNGRADE_FROM_MEDIUM_OR_MINIMUM,
        ]:
            return DecarceralImpactType.DOWNGRADE_SUPERVISION_LEVEL
        if self in [
            TaskCompletionEventType.OVERRIDE_TO_CONDITIONAL_LOW_RISK_SUPERVISION,
            TaskCompletionEventType.TRANSFER_TO_LIMITED_SUPERVISION,
            TaskCompletionEventType.TRANSFER_TO_LIMITED_SUPERVISION_2025_POLICY,
            TaskCompletionEventType.TRANSFER_TO_LIMITED_SUPERVISION_FROM_MINIMUM,
            TaskCompletionEventType.TRANSFER_TO_UNSUPERVISED_PAROLE,
            TaskCompletionEventType.TRANSFER_TO_SPECIAL_CIRCUMSTANCES_SUPERVISION,
            TaskCompletionEventType.TRANSFER_TO_ADMINISTRATIVE_SUPERVISION,
            TaskCompletionEventType.RELEASE_TO_LIMITED_SUPERVISION,
        ]:
            return DecarceralImpactType.TRANSFER_TO_LIMITED_SUPERVISION
        if self in [
            TaskCompletionEventType.TRANSFER_TO_NO_CONTACT_PAROLE,
        ]:
            return DecarceralImpactType.TRANSFER_TO_NO_CONTACT_SUPERVISION
        if self in [
            TaskCompletionEventType.GRANTED_SUPERVISION_SENTENCE_REDUCTION,
        ]:
            return DecarceralImpactType.CREDIT_REDUCTION_GRANTED
        raise ValueError(
            f"No decarceral impact type configured for completion event type {self.value}"
        )

    @property
    def is_jii_decarceral_transition(self) -> bool:
        """A boolean flag that is True if an event represents a meaningful transition in a JII’s
        lived experience toward greater liberty. This flag is False for any other event that helps
        facilitates the impact type, but doesn’t on its own represent a decarceral shift in a
        JII’s lived experience. For example, a release from restrictive housing event will have a
        True flag (because this represents a JII moving out of solitary confinement into a less
        carceral facility), whereas a review hearing to assess eligibility for release will have
        a False flag (because a hearing on its own doesn’t represent any movement for a JII, but
        does represent an action taken by actors in the criminal justice system that could
        eventually facilitate a JII transition). Similarly, an early discharge is a JII decarceral
        transition, whereas an early discharge request form submission is an intermediate staff
        action and does not count as a JII transition."""
        if self in [
            TaskCompletionEventType.CUSTODY_LEVEL_DOWNGRADE,
            TaskCompletionEventType.CUSTODY_LEVEL_DOWNGRADE_TO_MEDIUM_TRUSTEE,
            TaskCompletionEventType.CUSTODY_LEVEL_UPGRADE,
            TaskCompletionEventType.EARLY_DISCHARGE,
            TaskCompletionEventType.EARLY_RELEASE_TO_COMMUNITY_CONFINEMENT_SUPERVISION_NOT_OVERDUE,
            TaskCompletionEventType.EARLY_RELEASE_TO_COMMUNITY_CONFINEMENT_SUPERVISION_OVERDUE,
            TaskCompletionEventType.EARLY_RELEASE_TO_DRUG_PROGRAM_NOT_OVERDUE,
            TaskCompletionEventType.EARLY_RELEASE_TO_DRUG_PROGRAM_OVERDUE,
            TaskCompletionEventType.FULL_TERM_DISCHARGE,
            TaskCompletionEventType.GRANTED_FURLOUGH,
            TaskCompletionEventType.GRANTED_INSTITUTIONAL_WORKER_STATUS,
            TaskCompletionEventType.GRANTED_WORK_RELEASE,
            TaskCompletionEventType.OVERRIDE_TO_CONDITIONAL_LOW_RISK_SUPERVISION,
            TaskCompletionEventType.OVERRIDE_TO_LOW_SUPERVISION,
            TaskCompletionEventType.RELEASE_TO_COMMUNITY_CONFINEMENT_SUPERVISION,
            TaskCompletionEventType.RELEASE_TO_PAROLE,
            TaskCompletionEventType.SUPERVISION_LEVEL_DOWNGRADE,
            TaskCompletionEventType.SUPERVISION_LEVEL_DOWNGRADE_AFTER_INITIAL_CLASSIFICATION_REVIEW_DATE,
            TaskCompletionEventType.SUPERVISION_LEVEL_DOWNGRADE_BEFORE_INITIAL_CLASSIFICATION_REVIEW_DATE,
            TaskCompletionEventType.TRANSFER_OUT_OF_ADMINISTRATIVE_SOLITARY_CONFINEMENT,
            TaskCompletionEventType.TRANSFER_OUT_OF_DISCIPLINARY_OR_TEMPORARY_SOLITARY_CONFINEMENT,
            TaskCompletionEventType.TRANSFER_OUT_OF_SOLITARY_CONFINEMENT,
            TaskCompletionEventType.TRANSFER_TO_LIMITED_SUPERVISION,
            TaskCompletionEventType.TRANSFER_TO_LIMITED_SUPERVISION_2025_POLICY,
            TaskCompletionEventType.TRANSFER_TO_LIMITED_SUPERVISION_FROM_MINIMUM,
            TaskCompletionEventType.TRANSFER_TO_UNSUPERVISED_PAROLE,
            TaskCompletionEventType.TRANSFER_TO_MINIMUM_FACILITY,
            TaskCompletionEventType.TRANSFER_TO_NO_CONTACT_PAROLE,
            TaskCompletionEventType.TRANSFER_TO_SPECIAL_CIRCUMSTANCES_SUPERVISION,
            TaskCompletionEventType.TRANSFER_TO_ADMINISTRATIVE_SUPERVISION,
            TaskCompletionEventType.TRANSFER_TO_SUPERVISION_RUN_FACILITY,
            TaskCompletionEventType.RELEASE_TO_LIMITED_SUPERVISION,
            TaskCompletionEventType.SUPERVISION_LEVEL_DOWNGRADE_FROM_MEDIUM_OR_MINIMUM,
            TaskCompletionEventType.GRANTED_SUPERVISION_SENTENCE_REDUCTION,
        ]:
            return True
        if self in [
            TaskCompletionEventType.ADD_IN_PERSON_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW,
            TaskCompletionEventType.HEARING_OCCURRED,
            TaskCompletionEventType.INCARCERATION_INTAKE_ASSESSMENT_COMPLETED,
            TaskCompletionEventType.INCARCERATION_ASSESSMENT_COMPLETED,
            TaskCompletionEventType.KUDOS_SMS_SENT,
            TaskCompletionEventType.GOOD_TIME_REINSTATED,
            TaskCompletionEventType.REVIEW_HEARING_OCCURRED,
            TaskCompletionEventType.SECURITY_CLASSIFICATION_COMMITTEE_REVIEW,
            TaskCompletionEventType.WARDEN_IN_PERSON_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW,
            TaskCompletionEventType.FACE_TO_FACE_CONTACT,
        ]:
            return False
        raise ValueError(
            f"No JII decarceral transition flag configured for completion event type {self.value}"
        )

    @property
    def has_mandatory_due_date(self) -> bool:
        """A boolean flag that is True if there is a mandatory due date associated with the event
        indicating when the event is supposed to be completed by. Mandatory due dates include
        sentence projected full term release date (max), restrictive housing hearing dates,
        assessment/classification dates."""
        if self in [
            TaskCompletionEventType.ADD_IN_PERSON_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW,
            TaskCompletionEventType.FULL_TERM_DISCHARGE,
            TaskCompletionEventType.HEARING_OCCURRED,
            TaskCompletionEventType.INCARCERATION_INTAKE_ASSESSMENT_COMPLETED,
            TaskCompletionEventType.INCARCERATION_ASSESSMENT_COMPLETED,
            TaskCompletionEventType.REVIEW_HEARING_OCCURRED,
            TaskCompletionEventType.SECURITY_CLASSIFICATION_COMMITTEE_REVIEW,
            TaskCompletionEventType.TRANSFER_OUT_OF_DISCIPLINARY_OR_TEMPORARY_SOLITARY_CONFINEMENT,
            # TODO(#35491): move restrictive housing transitions w/o a mandatory date into a new event
            TaskCompletionEventType.TRANSFER_OUT_OF_SOLITARY_CONFINEMENT,
            TaskCompletionEventType.WARDEN_IN_PERSON_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW,
            TaskCompletionEventType.FACE_TO_FACE_CONTACT,
        ]:
            return True
        if self in [
            TaskCompletionEventType.CUSTODY_LEVEL_DOWNGRADE,
            TaskCompletionEventType.CUSTODY_LEVEL_DOWNGRADE_TO_MEDIUM_TRUSTEE,
            TaskCompletionEventType.CUSTODY_LEVEL_UPGRADE,
            TaskCompletionEventType.EARLY_DISCHARGE,
            TaskCompletionEventType.EARLY_RELEASE_TO_COMMUNITY_CONFINEMENT_SUPERVISION_NOT_OVERDUE,
            TaskCompletionEventType.EARLY_RELEASE_TO_COMMUNITY_CONFINEMENT_SUPERVISION_OVERDUE,
            TaskCompletionEventType.EARLY_RELEASE_TO_DRUG_PROGRAM_NOT_OVERDUE,
            TaskCompletionEventType.EARLY_RELEASE_TO_DRUG_PROGRAM_OVERDUE,
            TaskCompletionEventType.GOOD_TIME_REINSTATED,
            TaskCompletionEventType.GRANTED_FURLOUGH,
            TaskCompletionEventType.GRANTED_INSTITUTIONAL_WORKER_STATUS,
            TaskCompletionEventType.GRANTED_WORK_RELEASE,
            TaskCompletionEventType.KUDOS_SMS_SENT,
            TaskCompletionEventType.OVERRIDE_TO_CONDITIONAL_LOW_RISK_SUPERVISION,
            TaskCompletionEventType.OVERRIDE_TO_LOW_SUPERVISION,
            TaskCompletionEventType.RELEASE_TO_COMMUNITY_CONFINEMENT_SUPERVISION,
            TaskCompletionEventType.RELEASE_TO_PAROLE,
            TaskCompletionEventType.SUPERVISION_LEVEL_DOWNGRADE,
            TaskCompletionEventType.SUPERVISION_LEVEL_DOWNGRADE_AFTER_INITIAL_CLASSIFICATION_REVIEW_DATE,
            TaskCompletionEventType.SUPERVISION_LEVEL_DOWNGRADE_BEFORE_INITIAL_CLASSIFICATION_REVIEW_DATE,
            TaskCompletionEventType.TRANSFER_OUT_OF_ADMINISTRATIVE_SOLITARY_CONFINEMENT,
            TaskCompletionEventType.TRANSFER_TO_LIMITED_SUPERVISION,
            TaskCompletionEventType.TRANSFER_TO_LIMITED_SUPERVISION_2025_POLICY,
            TaskCompletionEventType.TRANSFER_TO_LIMITED_SUPERVISION_FROM_MINIMUM,
            TaskCompletionEventType.TRANSFER_TO_UNSUPERVISED_PAROLE,
            TaskCompletionEventType.TRANSFER_TO_MINIMUM_FACILITY,
            TaskCompletionEventType.TRANSFER_TO_NO_CONTACT_PAROLE,
            TaskCompletionEventType.TRANSFER_TO_SPECIAL_CIRCUMSTANCES_SUPERVISION,
            TaskCompletionEventType.TRANSFER_TO_ADMINISTRATIVE_SUPERVISION,
            TaskCompletionEventType.TRANSFER_TO_SUPERVISION_RUN_FACILITY,
            TaskCompletionEventType.RELEASE_TO_LIMITED_SUPERVISION,
            TaskCompletionEventType.SUPERVISION_LEVEL_DOWNGRADE_FROM_MEDIUM_OR_MINIMUM,
            TaskCompletionEventType.GRANTED_SUPERVISION_SENTENCE_REDUCTION,
        ]:
            return False
        raise ValueError(
            f"No mandatory date flag configured for completion event type {self.value}"
        )


class StateSpecificTaskCompletionEventBigQueryViewBuilder(SimpleBigQueryViewBuilder):
    """Defines a BigQueryViewBuilder that can be used to define task completion events
    of a given type. These views are used as inputs to a task eligibility spans view.
    """

    def __init__(
        self,
        state_code: StateCode,
        completion_event_type: TaskCompletionEventType,
        completion_event_query_template: str,
        description: str,
        **query_format_kwargs: str,
    ) -> None:
        view_id = completion_event_type.value.lower()
        super().__init__(
            dataset_id=completion_event_state_specific_dataset(state_code),
            view_id=view_id,
            description=description,
            view_query_template=completion_event_query_template,
            should_materialize=True,
            materialized_address_override=None,
            projects_to_deploy=None,
            clustering_fields=None,
            time_partitioning=None,
            materialized_table_schema=None,
            **query_format_kwargs,
        )
        self.completion_event_type = completion_event_type
        self.state_code = state_code
        self.task_type_name = completion_event_type.name
        self.task_title = self.task_type_name.replace("_", " ").title()


def exclude_states_in_state_specific_views(
    query_template: str,
    states_to_exclude: Optional[List[StateCode]] = None,
) -> str:
    """Wraps the base query in a CTE and excludes states that are specified in the view builder as having state-specific
    version of the completion event"""

    if states_to_exclude:
        state_code_query_fragment = f"\nWHERE state_code NOT IN  ({list_to_query_string([x.name for x in states_to_exclude], quoted=True)})"
    else:
        state_code_query_fragment = ""
    return f"""
    WITH base_query AS (
    {query_template.rstrip().rstrip(";")}
    )
    SELECT * FROM base_query{state_code_query_fragment}
    """


class StateAgnosticTaskCompletionEventBigQueryViewBuilder(SimpleBigQueryViewBuilder):
    """Defines a BigQueryViewBuilder that can be used to define task completion events
    of a given type. These views are used as inputs to a task eligibility spans view.
    """

    def __init__(
        self,
        completion_event_type: TaskCompletionEventType,
        completion_event_query_template: str,
        description: str,
        states_to_exclude: Optional[List[StateCode]] = None,
        **query_format_kwargs: str,
    ) -> None:
        view_id = completion_event_type.value.lower()
        super().__init__(
            dataset_id=TASK_COMPLETION_EVENTS_DATASET_ID,
            view_id=view_id,
            description=description,
            view_query_template=exclude_states_in_state_specific_views(
                completion_event_query_template, states_to_exclude
            ),
            should_materialize=True,
            materialized_address_override=None,
            projects_to_deploy=None,
            clustering_fields=None,
            time_partitioning=None,
            materialized_table_schema=None,
            **query_format_kwargs,
        )
        self.completion_event_type = completion_event_type
        self.task_type_name = completion_event_type.name
        self.task_title = self.task_type_name.replace("_", " ").title()
        self.states_to_exclude = states_to_exclude


TaskCompletionEventBigQueryViewBuilder = Union[
    StateAgnosticTaskCompletionEventBigQueryViewBuilder,
    StateSpecificTaskCompletionEventBigQueryViewBuilder,
]
