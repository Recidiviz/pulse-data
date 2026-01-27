# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Test utils for generating state Entity classes."""

import datetime
import unittest
from typing import Optional, Sequence

from recidiviz.common.constants.state.external_id_types import US_ND_ELITE
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentClass,
    StateAssessmentLevel,
    StateAssessmentType,
)
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_charge import (
    StateChargeClassificationType,
    StateChargeStatus,
    StateChargeV2ClassificationType,
    StateChargeV2Status,
)
from recidiviz.common.constants.state.state_drug_screen import (
    StateDrugScreenResult,
    StateDrugScreenSampleType,
)
from recidiviz.common.constants.state.state_early_discharge import (
    StateEarlyDischargeDecision,
)
from recidiviz.common.constants.state.state_employment_period import (
    StateEmploymentPeriodEmploymentStatus,
    StateEmploymentPeriodEndReason,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_incident import (
    StateIncarcerationIncidentOutcomeType,
    StateIncarcerationIncidentType,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_person import StateEthnicity, StateRace
from recidiviz.common.constants.state.state_person_address_period import (
    StatePersonAddressType,
)
from recidiviz.common.constants.state.state_person_housing_status_period import (
    StatePersonHousingStatusType,
)
from recidiviz.common.constants.state.state_person_staff_relationship_period import (
    StatePersonStaffRelationshipType,
)
from recidiviz.common.constants.state.state_program_assignment import (
    StateProgramAssignmentParticipationStatus,
)
from recidiviz.common.constants.state.state_scheduled_supervision_contact import (
    StateScheduledSupervisionContactLocation,
    StateScheduledSupervisionContactMethod,
    StateScheduledSupervisionContactReason,
    StateScheduledSupervisionContactStatus,
    StateScheduledSupervisionContactType,
)
from recidiviz.common.constants.state.state_sentence import (
    StateSentenceStatus,
    StateSentenceType,
    StateSentencingAuthority,
)
from recidiviz.common.constants.state.state_shared_enums import StateActingBodyType
from recidiviz.common.constants.state.state_staff_caseload_type import (
    StateStaffCaseloadType,
)
from recidiviz.common.constants.state.state_staff_role_period import (
    StateStaffRoleSubtype,
    StateStaffRoleType,
)
from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactLocation,
    StateSupervisionContactReason,
    StateSupervisionContactStatus,
    StateSupervisionContactType,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.common.constants.state.state_supervision_sentence import (
    StateSupervisionSentenceSupervisionType,
)
from recidiviz.common.constants.state.state_supervision_violated_condition import (
    StateSupervisionViolatedConditionType,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecidingBodyType,
    StateSupervisionViolationResponseDecision,
    StateSupervisionViolationResponseType,
)
from recidiviz.common.constants.state.state_system_type import StateSystemType
from recidiviz.common.constants.state.state_task_deadline import StateTaskType
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_entity,
    entities_module_context_for_module,
)
from recidiviz.persistence.entity.entity_field_index import EntityFieldType
from recidiviz.persistence.entity.entity_utils import (
    get_all_entities_from_tree,
    get_all_entity_classes_in_module,
    set_backedges,
)
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state import normalized_entities
from recidiviz.persistence.entity.state.entities import (
    StateProgramAssignment,
    StateSupervisionViolationResponse,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateSupervisionViolatedConditionEntry,
    NormalizedStateSupervisionViolation,
    NormalizedStateSupervisionViolationResponse,
    NormalizedStateSupervisionViolationResponseDecisionEntry,
    NormalizedStateSupervisionViolationTypeEntry,
)
from recidiviz.utils.types import assert_type


def clear_db_ids(db_entities: Sequence[Entity]) -> None:
    """Clears primary key fields off of all entities in all of the provided
    |db_entities| graphs.
    """
    for entity in db_entities:
        entities_module_context = entities_module_context_for_entity(entity)
        field_index = entities_module_context.field_index()
        entity.clear_id()
        for field_name in field_index.get_fields_with_non_empty_values(
            entity, EntityFieldType.FORWARD_EDGE
        ):
            clear_db_ids(entity.get_field_as_list(field_name))


def hydrate_bidirectional_relationships_on_expected_response(
    expected_response: StateSupervisionViolationResponse,
) -> None:
    """Hydrates all bi-directional relationships in the
    StateSupervisionViolationResponse subtree. For use in tests that need the full
    entity graph to be connected."""
    if expected_response.supervision_violation:
        for (
            type_entry
        ) in expected_response.supervision_violation.supervision_violation_types:
            type_entry.supervision_violation = expected_response.supervision_violation
        for (
            condition
        ) in expected_response.supervision_violation.supervision_violated_conditions:
            condition.supervision_violation = expected_response.supervision_violation

        expected_response.supervision_violation.supervision_violation_responses = [
            expected_response
        ]

    for decision in expected_response.supervision_violation_response_decisions:
        decision.supervision_violation_response = expected_response


def _hydrate_bidirectional_relationships_on_expected_normalized_response(
    expected_response: NormalizedStateSupervisionViolationResponse,
) -> None:
    """Hydrates all bi-directional relationships in the
    NormalizedStateSupervisionViolationResponse subtree. For use in tests that need the
    full entity graph to be connected."""
    if expected_response.supervision_violation:
        for (
            type_entry
        ) in expected_response.supervision_violation.supervision_violation_types:
            type_entry.supervision_violation = expected_response.supervision_violation
        for (
            condition
        ) in expected_response.supervision_violation.supervision_violated_conditions:
            condition.supervision_violation = expected_response.supervision_violation

        expected_response.supervision_violation.supervision_violation_responses = [
            expected_response
        ]

    for decision in expected_response.supervision_violation_response_decisions:
        decision.supervision_violation_response = expected_response


def get_violation_tree(
    starting_id_value: Optional[int] = None,
) -> entities.StateSupervisionViolation:
    """Returns a tree of entities connected to the StateSupervisionViolation
    for use in tests that normalize violation data.

    DO NOT UPDATE THIS WITHOUT ALSO UPDATING get_normalized_violation_tree.
    """
    starting_id_value = starting_id_value or 1

    supervision_violation = entities.StateSupervisionViolation.new_with_defaults(
        external_id="sv1",
        supervision_violation_id=starting_id_value,
        violation_date=datetime.date(year=2004, month=9, day=1),
        state_code="US_XX",
        is_violent=False,
        supervision_violation_types=[
            entities.StateSupervisionViolationTypeEntry.new_with_defaults(
                supervision_violation_type_entry_id=starting_id_value + 1,
                state_code="US_XX",
                violation_type=entities.StateSupervisionViolationType.TECHNICAL,
                violation_type_raw_text="TECHNICAL",
            ),
        ],
        supervision_violated_conditions=[
            entities.StateSupervisionViolatedConditionEntry.new_with_defaults(
                supervision_violated_condition_entry_id=starting_id_value + 2,
                state_code="US_XX",
                condition=StateSupervisionViolatedConditionType.SPECIAL_CONDITIONS,
                condition_raw_text="MISSED CURFEW",
            )
        ],
    )

    supervision_violation_response_1 = entities.StateSupervisionViolationResponse.new_with_defaults(
        external_id="svr1",
        supervision_violation_response_id=starting_id_value + 3,
        response_type=entities.StateSupervisionViolationResponseType.CITATION,
        response_date=datetime.date(year=2004, month=9, day=2),
        state_code="US_XX",
        deciding_body_type=entities.StateSupervisionViolationResponseDecidingBodyType.SUPERVISION_OFFICER,
        supervision_violation=supervision_violation,
        supervision_violation_response_decisions=[
            entities.StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code="US_XX",
                supervision_violation_response_decision_entry_id=starting_id_value + 4,
                decision=StateSupervisionViolationResponseDecision.EXTERNAL_UNKNOWN,
                decision_raw_text="X",
            )
        ],
    )

    supervision_violation_response_2 = entities.StateSupervisionViolationResponse.new_with_defaults(
        external_id="svr2",
        supervision_violation_response_id=starting_id_value + 5,
        response_type=entities.StateSupervisionViolationResponseType.VIOLATION_REPORT,
        response_date=datetime.date(year=2004, month=10, day=3),
        state_code="US_XX",
        deciding_body_type=entities.StateSupervisionViolationResponseDecidingBodyType.SUPERVISION_OFFICER,
        supervision_violation=supervision_violation,
        supervision_violation_response_decisions=[
            entities.StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code="US_XX",
                supervision_violation_response_decision_entry_id=starting_id_value + 6,
                decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
                decision_raw_text="Y",
            )
        ],
    )

    hydrate_bidirectional_relationships_on_expected_response(
        supervision_violation_response_1
    )

    hydrate_bidirectional_relationships_on_expected_response(
        supervision_violation_response_2
    )

    supervision_violation.supervision_violation_responses = [
        supervision_violation_response_1,
        supervision_violation_response_2,
    ]

    return supervision_violation


def get_normalized_violation_tree(
    starting_id_value: Optional[int] = 0, starting_sequence_num: Optional[int] = 0
) -> NormalizedStateSupervisionViolation:
    """Returns a tree of normalized versions of the entities returned by
    _get_violation_response_tree."""
    starting_id_value = starting_id_value or 1
    starting_sequence_num = starting_sequence_num or 0

    supervision_violation = NormalizedStateSupervisionViolation(
        external_id="sv1",
        supervision_violation_id=starting_id_value,
        violation_date=datetime.date(year=2004, month=9, day=1),
        state_code="US_XX",
        is_violent=False,
        supervision_violation_types=[
            NormalizedStateSupervisionViolationTypeEntry(
                state_code="US_XX",
                supervision_violation_type_entry_id=starting_id_value + 1,
                violation_type=entities.StateSupervisionViolationType.TECHNICAL,
                violation_type_raw_text="TECHNICAL",
            ),
        ],
        supervision_violated_conditions=[
            NormalizedStateSupervisionViolatedConditionEntry(
                supervision_violated_condition_entry_id=starting_id_value + 2,
                state_code="US_XX",
                condition=StateSupervisionViolatedConditionType.SPECIAL_CONDITIONS,
                condition_raw_text="MISSED CURFEW",
            )
        ],
    )

    supervision_violation_response_1 = NormalizedStateSupervisionViolationResponse(
        external_id="svr1",
        supervision_violation_response_id=starting_id_value + 3,
        response_type=entities.StateSupervisionViolationResponseType.CITATION,
        response_date=datetime.date(year=2004, month=9, day=2),
        state_code="US_XX",
        deciding_body_type=entities.StateSupervisionViolationResponseDecidingBodyType.SUPERVISION_OFFICER,
        sequence_num=starting_sequence_num,
        supervision_violation=supervision_violation,
        supervision_violation_response_decisions=[
            NormalizedStateSupervisionViolationResponseDecisionEntry(
                state_code="US_XX",
                supervision_violation_response_decision_entry_id=starting_id_value + 4,
                decision=StateSupervisionViolationResponseDecision.EXTERNAL_UNKNOWN,
                decision_raw_text="X",
            )
        ],
    )

    supervision_violation_response_2 = NormalizedStateSupervisionViolationResponse(
        external_id="svr2",
        supervision_violation_response_id=starting_id_value + 5,
        response_type=entities.StateSupervisionViolationResponseType.VIOLATION_REPORT,
        response_date=datetime.date(year=2004, month=10, day=3),
        state_code="US_XX",
        deciding_body_type=entities.StateSupervisionViolationResponseDecidingBodyType.SUPERVISION_OFFICER,
        sequence_num=starting_sequence_num + 1,
        supervision_violation=supervision_violation,
        supervision_violation_response_decisions=[
            NormalizedStateSupervisionViolationResponseDecisionEntry(
                state_code="US_XX",
                supervision_violation_response_decision_entry_id=starting_id_value + 6,
                decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
                decision_raw_text="Y",
            )
        ],
    )

    _hydrate_bidirectional_relationships_on_expected_normalized_response(
        supervision_violation_response_1
    )
    _hydrate_bidirectional_relationships_on_expected_normalized_response(
        supervision_violation_response_2
    )

    supervision_violation.supervision_violation_responses = [
        supervision_violation_response_1,
        supervision_violation_response_2,
    ]

    return supervision_violation


def generate_full_graph_state_person(
    set_back_edges: bool,
    include_person_back_edges: bool = True,
    set_ids: bool = False,
) -> entities.StatePerson:
    """Test util for generating a StatePerson that has at least one child of
    each possible Entity type, with all possible edge types defined between
    objects.

    Args:
        set_back_edges: explicitly sets all the back edges on the graph
            that will get automatically filled in when this entity graph is
            written to the DB.
        include_person_back_edges: If set_back_edges is set to True, whether or not to
            set the back edges to StatePerson. This is usually False when testing
            calculation pipelines that do not hydrate this edge.
        set_ids: It True, sets a value on the entity id field (primary key) of each
            entity.

    Returns:
        A test instance of a StatePerson.
    """

    entities_module_context = entities_module_context_for_module(state_entities)
    person = entities.StatePerson.new_with_defaults(state_code="US_XX")

    person.external_ids = [
        entities.StatePersonExternalId.new_with_defaults(
            state_code="US_XX",
            external_id="ELITE_ID_123",
            id_type=US_ND_ELITE,
        )
    ]
    person.aliases = [
        entities.StatePersonAlias.new_with_defaults(
            state_code="US_XX",
            full_name="Beyoncé Giselle Knowles",
        ),
        entities.StatePersonAlias.new_with_defaults(
            state_code="US_XX",
            full_name="Beyoncé Giselle Knowles-Carter",
        ),
    ]

    person.races = [
        entities.StatePersonRace.new_with_defaults(
            state_code="US_XX", race=StateRace.ASIAN, race_raw_text="ASIAN"
        ),
        entities.StatePersonRace.new_with_defaults(
            state_code="US_XX", race=StateRace.BLACK, race_raw_text="BLACK"
        ),
    ]

    assessment1 = entities.StateAssessment.new_with_defaults(
        external_id="a1",
        assessment_class=StateAssessmentClass.RISK,
        assessment_class_raw_text=None,
        assessment_type=StateAssessmentType.LSIR,
        assessment_type_raw_text="LSIR",
        assessment_date=datetime.date(2003, month=8, day=10),
        state_code="US_XX",
        assessment_score=55,
        assessment_level=StateAssessmentLevel.MEDIUM,
        assessment_level_raw_text="MED",
        assessment_metadata="assessment metadata",
        conducting_staff_external_id="EMP3",
        conducting_staff_external_id_type="US_XX_STAFF_ID",
    )

    assessment2 = entities.StateAssessment.new_with_defaults(
        external_id="a2",
        assessment_class=StateAssessmentClass.RISK,
        assessment_class_raw_text=None,
        assessment_type=StateAssessmentType.LSIR,
        assessment_type_raw_text="LSIR",
        assessment_date=datetime.date(2004, month=9, day=10),
        state_code="US_XX",
        assessment_score=10,
        assessment_level=StateAssessmentLevel.LOW,
        assessment_level_raw_text="LOW",
        assessment_metadata="more assessment metadata",
        conducting_staff_external_id="EMP3",
        conducting_staff_external_id_type="US_XX_STAFF_ID",
    )

    person.assessments = [assessment1, assessment2]

    program_assignment = StateProgramAssignment.new_with_defaults(
        external_id="program_assignment_external_id_1",
        state_code="US_XX",
        participation_status=StateProgramAssignmentParticipationStatus.IN_PROGRESS,
        participation_status_raw_text="IN_PROGRESS",
        referral_date=datetime.date(year=2019, month=2, day=10),
        start_date=datetime.date(year=2019, month=2, day=11),
        program_id="program_id",
        program_location_id="program_location_id",
        referring_staff_external_id="EMP1",
        referring_staff_external_id_type="US_XX_STAFF_ID",
    )

    program_assignment2 = StateProgramAssignment.new_with_defaults(
        external_id="program_assignment_external_id_2",
        state_code="US_XX",
        participation_status=StateProgramAssignmentParticipationStatus.DISCHARGED_UNKNOWN,
        participation_status_raw_text="DISCHARGED_UNKNOWN",
        referral_date=datetime.date(year=2019, month=2, day=10),
        start_date=datetime.date(year=2019, month=2, day=11),
        discharge_date=datetime.date(year=2019, month=2, day=12),
        program_id="program_id",
        program_location_id="program_location_id",
        referring_staff_external_id="EMP1",
        referring_staff_external_id_type="US_XX_STAFF_ID",
    )

    person.program_assignments = [program_assignment, program_assignment2]

    incident_outcome = entities.StateIncarcerationIncidentOutcome.new_with_defaults(
        external_id="io1",
        outcome_type=StateIncarcerationIncidentOutcomeType.WARNING,
        outcome_type_raw_text="WARNING",
        date_effective=datetime.date(year=2003, month=8, day=20),
        state_code="US_XX",
        outcome_description="LOSS OF COMMISSARY",
        punishment_length_days=30,
    )

    incarceration_incident = entities.StateIncarcerationIncident.new_with_defaults(
        external_id="i1",
        incident_type=StateIncarcerationIncidentType.CONTRABAND,
        incident_type_raw_text="CONTRABAND",
        incident_date=datetime.date(year=2003, month=8, day=10),
        state_code="US_XX",
        facility="ALCATRAZ",
        location_within_facility="13B",
        incident_details="Found contraband cell phone.",
        incarceration_incident_outcomes=[incident_outcome],
    )

    person.incarceration_incidents = [incarceration_incident]

    supervision_violation = entities.StateSupervisionViolation.new_with_defaults(
        external_id="sv1",
        violation_date=datetime.date(year=2004, month=9, day=1),
        state_code="US_XX",
        is_violent=False,
        supervision_violation_types=[
            entities.StateSupervisionViolationTypeEntry.new_with_defaults(
                state_code="US_XX",
                violation_type=StateSupervisionViolationType.TECHNICAL,
                violation_type_raw_text="TECHNICAL",
            ),
        ],
        supervision_violated_conditions=[
            entities.StateSupervisionViolatedConditionEntry.new_with_defaults(
                state_code="US_XX",
                condition=StateSupervisionViolatedConditionType.SPECIAL_CONDITIONS,
                condition_raw_text="MISSED CURFEW",
            )
        ],
    )

    person.supervision_violations = [supervision_violation]

    supervision_contact = entities.StateSupervisionContact.new_with_defaults(
        external_id="CONTACT_ID",
        status=StateSupervisionContactStatus.COMPLETED,
        status_raw_text="COMPLETED",
        contact_type=StateSupervisionContactType.DIRECT,
        contact_type_raw_text="FACE_TO_FACE",
        contact_date=datetime.date(year=1111, month=1, day=2),
        state_code="US_XX",
        contact_reason=StateSupervisionContactReason.GENERAL_CONTACT,
        contact_reason_raw_text="GENERAL_CONTACT",
        location=StateSupervisionContactLocation.RESIDENCE,
        location_raw_text="RESIDENCE",
        verified_employment=True,
        resulted_in_arrest=False,
        contacting_staff_external_id="EMP2",
        contacting_staff_external_id_type="US_XX_STAFF_ID",
    )
    person.supervision_contacts = [supervision_contact]

    scheduled_supervision_contact = (
        entities.StateScheduledSupervisionContact.new_with_defaults(
            external_id="SCHEDULED_CONTACT_ID",
            status=StateScheduledSupervisionContactStatus.SCHEDULED,
            status_raw_text="SCHEDULED",
            contact_type=StateScheduledSupervisionContactType.DIRECT,
            contact_type_raw_text="FACE_TO_FACE",
            scheduled_contact_date=datetime.date(year=2020, month=1, day=2),
            state_code="US_XX",
            update_datetime=datetime.datetime(2020, 1, 1),
            contact_reason=StateScheduledSupervisionContactReason.GENERAL_CONTACT,
            contact_reason_raw_text="GENERAL_CONTACT",
            location=StateScheduledSupervisionContactLocation.RESIDENCE,
            location_raw_text="RESIDENCE",
            contacting_staff_external_id="EMP2",
            contacting_staff_external_id_type="US_XX_STAFF_ID",
            sequence_num=1,
            contact_method=StateScheduledSupervisionContactMethod.IN_PERSON,
            contact_method_raw_text="CONTACT",
        )
    )
    person.scheduled_supervision_contacts = [scheduled_supervision_contact]

    supervision_violation_response = entities.StateSupervisionViolationResponse.new_with_defaults(
        external_id="svr1",
        response_type=StateSupervisionViolationResponseType.CITATION,
        response_date=datetime.date(year=2004, month=9, day=2),
        state_code="US_XX",
        deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.SUPERVISION_OFFICER,
    )

    supervision_violation.supervision_violation_responses = [
        supervision_violation_response
    ]

    violation_response_decision = (
        entities.StateSupervisionViolationResponseDecisionEntry(
            state_code="US_XX",
            decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
            decision_raw_text="PR",
        )
    )
    supervision_violation_response.supervision_violation_response_decisions = [
        violation_response_decision
    ]

    incarceration_sentence = entities.StateIncarcerationSentence.new_with_defaults(
        external_id="BOOK_ID1234-1",
        status=StateSentenceStatus.COMPLETED,
        status_raw_text="COMPLETED",
        incarceration_type=StateIncarcerationType.STATE_PRISON,
        incarceration_type_raw_text="PRISON",
        date_imposed=datetime.date(year=2018, month=7, day=3),
        projected_min_release_date=datetime.date(year=2017, month=5, day=14),
        projected_max_release_date=None,
        parole_eligibility_date=datetime.date(year=2018, month=5, day=14),
        state_code="US_XX",
        county_code="US_XX_COUNTY",
        #   - What
        # These will be None if is_life is true
        min_length_days=90,
        max_length_days=900,
        is_life=False,
        is_capital_punishment=False,
        parole_possible=True,
        initial_time_served_days=None,
        good_time_days=10,
        earned_time_days=None,
    )

    supervision_sentence = entities.StateSupervisionSentence.new_with_defaults(
        external_id="BOOK_ID1234-2",
        status=StateSentenceStatus.SERVING,
        status_raw_text="SERVING",
        supervision_type=StateSupervisionSentenceSupervisionType.PAROLE,
        supervision_type_raw_text="PAROLE",
        projected_completion_date=datetime.date(year=2020, month=5, day=14),
        completion_date=None,
        state_code="US_XX",
        min_length_days=None,
        max_length_days=200,
    )

    person.incarceration_sentences = [incarceration_sentence]
    person.supervision_sentences = [supervision_sentence]

    incarceration_period = entities.StateIncarcerationPeriod.new_with_defaults(
        external_id="ip1",
        incarceration_type=StateIncarcerationType.STATE_PRISON,
        incarceration_type_raw_text=None,
        admission_date=datetime.date(year=2003, month=8, day=1),
        release_date=datetime.date(year=2004, month=8, day=1),
        state_code="US_XX",
        county_code="US_XX_COUNTY",
        facility="ALCATRAZ",
        housing_unit="BLOCK A",
        admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        admission_reason_raw_text="NEW ADMISSION",
        release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        release_reason_raw_text="CONDITIONAL RELEASE",
        specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
    )

    person.incarceration_periods = [incarceration_period]

    charge = entities.StateCharge.new_with_defaults(
        external_id="CHARGE1_EXTERNAL_ID",
        status=StateChargeStatus.CONVICTED,
        status_raw_text="CONVICTED",
        offense_date=datetime.date(year=2003, month=7, day=1),
        date_charged=datetime.date(year=2003, month=8, day=1),
        state_code="US_XX",
        statute="A102.3",
        description="DRUG POSSESSION",
        attempted=True,
        classification_type=StateChargeClassificationType.FELONY,
        classification_type_raw_text="F",
        classification_subtype="A",
        counts=1,
        charge_notes=None,
    )

    charge2 = entities.StateCharge.new_with_defaults(
        external_id="CHARGE2_EXTERNAL_ID",
        status=StateChargeStatus.CONVICTED,
        status_raw_text="CONVICTED",
        offense_date=datetime.date(year=2003, month=7, day=1),
        date_charged=datetime.date(year=2003, month=8, day=1),
        state_code="US_XX",
        statute="A102.3",
        description="DRUG POSSESSION",
        attempted=True,
        classification_type=StateChargeClassificationType.FELONY,
        classification_type_raw_text="F",
        classification_subtype="B",
        counts=1,
        charge_notes=None,
    )

    charge3 = entities.StateCharge.new_with_defaults(
        external_id="CHARGE3_EXTERNAL_ID",
        status=StateChargeStatus.DROPPED,
        status_raw_text="DROPPED",
        offense_date=datetime.date(year=2003, month=7, day=1),
        date_charged=datetime.date(year=2003, month=8, day=1),
        state_code="US_XX",
        statute="A102.3",
        description="DRUG POSSESSION",
        attempted=True,
        classification_type=StateChargeClassificationType.FELONY,
        classification_type_raw_text="F",
        classification_subtype="AA",
        counts=1,
        charge_notes=None,
    )

    supervision_sentence.charges = [charge, charge2, charge3]
    incarceration_sentence.charges = [charge, charge2, charge3]

    early_discharge_1 = entities.StateEarlyDischarge.new_with_defaults(
        external_id="ed1",
        request_date=datetime.date(year=2001, month=7, day=1),
        decision_date=datetime.date(year=2001, month=7, day=20),
        decision=StateEarlyDischargeDecision.SENTENCE_TERMINATION_GRANTED,
        decision_raw_text="approved",
        deciding_body_type=StateActingBodyType.PAROLE_BOARD,
        deciding_body_type_raw_text="pb",
        requesting_body_type=StateActingBodyType.SENTENCED_PERSON,
        requesting_body_type_raw_text="sentenced_person",
        state_code="US_XX",
        county_code="COUNTY",
    )
    early_discharge_2 = entities.StateEarlyDischarge.new_with_defaults(
        external_id="ed2",
        request_date=datetime.date(year=2002, month=7, day=1),
        decision_date=datetime.date(year=2002, month=7, day=20),
        decision=StateEarlyDischargeDecision.UNSUPERVISED_PROBATION_GRANTED,
        decision_raw_text="conditionally_approved",
        deciding_body_type=StateActingBodyType.COURT,
        deciding_body_type_raw_text="c",
        requesting_body_type=StateActingBodyType.SENTENCED_PERSON,
        requesting_body_type_raw_text="sentenced_person",
        state_code="US_XX",
        county_code="COUNTY",
    )
    early_discharge_3 = entities.StateEarlyDischarge.new_with_defaults(
        external_id="ed3",
        request_date=datetime.date(year=2001, month=1, day=1),
        decision_date=datetime.date(year=2001, month=1, day=20),
        decision=StateEarlyDischargeDecision.REQUEST_DENIED,
        decision_raw_text="denied",
        deciding_body_type=StateActingBodyType.PAROLE_BOARD,
        deciding_body_type_raw_text="pb",
        requesting_body_type=StateActingBodyType.SENTENCED_PERSON,
        requesting_body_type_raw_text="sentenced_person",
        state_code="US_XX",
        county_code="COUNTY",
    )
    supervision_sentence.early_discharges = [early_discharge_1, early_discharge_2]
    incarceration_sentence.early_discharges = [early_discharge_3]

    supervision_case_type_entry = (
        entities.StateSupervisionCaseTypeEntry.new_with_defaults(
            state_code="US_XX",
            case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
            case_type_raw_text="DOMESTIC_VIOLENCE",
        )
    )

    supervision_period = entities.StateSupervisionPeriod.new_with_defaults(
        external_id="sp1",
        supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        supervision_type_raw_text="PAROLE",
        start_date=datetime.date(year=2004, month=8, day=1),
        termination_date=None,
        state_code="US_XX",
        admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
        admission_reason_raw_text="RELEASE",
        termination_reason=None,
        termination_reason_raw_text=None,
        supervision_level=StateSupervisionLevel.EXTERNAL_UNKNOWN,
        supervision_level_raw_text="UNKNOWN",
        conditions="10PM CURFEW",
        case_type_entries=[supervision_case_type_entry],
        supervising_officer_staff_external_id="EMP1",
        supervising_officer_staff_external_id_type="US_XX_STAFF_ID",
    )

    person.supervision_periods = [supervision_period]

    task_deadline = entities.StateTaskDeadline(
        state_code="US_XX",
        task_type=StateTaskType.DISCHARGE_FROM_INCARCERATION,
        eligible_date=datetime.date(2020, 9, 11),
        update_datetime=datetime.datetime(2023, 2, 1, 11, 19),
        task_metadata='{"external_id": "00000001-111123-371006", "sentence_type": "INCARCERATION"}',
        sequence_num=None,
    )
    person.task_deadlines = [task_deadline]

    sentence = entities.StateSentence.new_with_defaults(
        state_code="US_XX",
        external_id="SENTENCE_EXTERNAL_ID_1",
        sentence_group_external_id="SENTENCE_GROUP_EXTERNAL_ID_1",
        sentence_metadata='{"BS_CCI": "", "BS_CRQ": "0", "SENTENCE_FLAG": "SENTENCE: 120 DAY"}',
        sentencing_authority=StateSentencingAuthority.COUNTY,
        sentence_type=StateSentenceType.STATE_PRISON,
        imposed_date=datetime.date(year=1956, month=3, day=16),
        is_life=True,
        is_capital_punishment=True,
    )
    person.sentences = [sentence]
    charge_v2 = entities.StateChargeV2.new_with_defaults(
        state_code="US_XX",
        status=StateChargeV2Status.PRESENT_WITHOUT_INFO,
        external_id="CHARGE_V2_EXTERNAL_ID_1",
        county_code="US_MO_JACKSON",
        ncic_code="0904",
        statute="10021040",
        description="TC: MURDER 1ST - FIST",
        is_violent=True,
        classification_type=StateChargeV2ClassificationType.FELONY,
        classification_type_raw_text="F",
        classification_subtype="O",
        judicial_district_code="22",
    )
    sentence.charges = [charge_v2]

    sentence_status_snapshot = entities.StateSentenceStatusSnapshot(
        state_code="US_XX",
        status=StateSentenceStatus.SERVING,
        status_update_datetime=datetime.datetime(2023, 1, 1),
        sequence_num=None,
    )
    sentence.sentence_status_snapshots = [sentence_status_snapshot]

    sentence_length = entities.StateSentenceLength(
        state_code="US_XX",
        length_update_datetime=datetime.datetime(2023, 1, 1),
        sequence_num=None,
    )
    sentence.sentence_lengths = [sentence_length]

    sentence_group = entities.StateSentenceGroup(
        state_code="US_XX",
        external_id=assert_type(sentence.sentence_group_external_id, str),
    )
    sentence_group_length = entities.StateSentenceGroupLength(
        state_code="US_XX",
        group_update_datetime=datetime.datetime(2023, 1, 1),
        sequence_num=None,
    )
    sentence_group.sentence_group_lengths = [sentence_group_length]

    person.sentence_groups = [sentence_group]

    drug_screen = entities.StateDrugScreen(
        state_code="US_XX",
        external_id="12356",
        drug_screen_date=datetime.date(2022, 5, 8),
        drug_screen_result=StateDrugScreenResult.NEGATIVE,
        drug_screen_result_raw_text="DRUN",
        sample_type=StateDrugScreenSampleType.BREATH,
        sample_type_raw_text="BREATH",
        drug_screen_metadata='{"DRUGTYPE": "METH"}',
    )
    person.drug_screens = [drug_screen]

    employment_period = entities.StateEmploymentPeriod(
        state_code="US_XX",
        external_id="12356",
        start_date=datetime.date(2022, 5, 8),
        end_date=datetime.date(2022, 5, 10),
        last_verified_date=datetime.date(2022, 5, 1),
        employment_status=StateEmploymentPeriodEmploymentStatus.EMPLOYED_PART_TIME,
        employment_status_raw_text="PT",
        end_reason=StateEmploymentPeriodEndReason.QUIT,
        end_reason_raw_text="PERSONAL",
        employer_name="ACME, INC.",
        employer_address="123 FAKE ST, ANYTOWN, XX, 00000",
        job_title=None,
    )
    person.employment_periods = [employment_period]

    address_period = entities.StatePersonAddressPeriod(
        state_code="US_XX",
        address_line_1="123 SANTA STREET",
        address_line_2="APT 4",
        address_city="NORTH POLE",
        address_zip="10000",
        address_county="GLACIER COUNTY",
        address_type=StatePersonAddressType.PHYSICAL_RESIDENCE,
        address_start_date=datetime.date(2020, 1, 1),
    )
    person.address_periods = [address_period]

    housing_status_period = entities.StatePersonHousingStatusPeriod(
        state_code="US_XX",
        housing_status_start_date=datetime.date(year=2006, month=7, day=2),
        housing_status_end_date=datetime.date(year=2007, month=7, day=2),
        housing_status_type=StatePersonHousingStatusType.PERMANENT_RESIDENCE,
    )
    person.housing_status_periods = [housing_status_period]

    staff_relationship = entities.StatePersonStaffRelationshipPeriod(
        state_code=StateCode.US_XX.value,
        relationship_start_date=datetime.date(2021, 1, 1),
        relationship_end_date_exclusive=datetime.date(2023, 1, 1),
        system_type=StateSystemType.INCARCERATION,
        relationship_type=StatePersonStaffRelationshipType.CASE_MANAGER,
        associated_staff_external_id="EMP2",
        associated_staff_external_id_type="US_XX_STAFF_ID",
        relationship_type_raw_text=None,
        relationship_priority=1,
    )
    person.staff_relationship_periods = [staff_relationship]

    if set_back_edges:
        set_backedges(person, entities_module_context)

    all_entities = get_all_entities_from_tree(person, entities_module_context)

    if not include_person_back_edges and set_back_edges:
        for entity in all_entities:
            if isinstance(entity, entities.StatePerson):
                continue
            entity.set_field("person", None)

    if set_ids:
        for entity in all_entities:
            if entity.get_id():
                raise ValueError(
                    f"Found entity [{entity}] with already set id field."
                    f"Not expected to be set until this part of the "
                    f"function."
                )
            id_name = entity.get_class_id_name()
            entity.set_field(id_name, id(entity))

    return person


def generate_full_graph_state_staff(
    set_back_edges: bool,
    set_ids: Optional[bool] = False,
) -> entities.StateStaff:
    """Test util for generating a StateStaff that has at least one child of
    each possible Entity type, with all possible edge types defined between
    objects.

    Args:
        set_back_edges: explicitly sets all the back edges on the graph
            that will get automatically filled in when this entity graph is
            written to the DB.
        set_ids: It True, sets a value on the entity id field (primary key) of each
            entity.

    Returns:
        A test instance of a StateStaff."""
    entities_module_context = entities_module_context_for_module(state_entities)
    staff = entities.StateStaff.new_with_defaults(
        state_code="US_XX", full_name="Staff Name", email="staff@staff.com"
    )

    staff.external_ids = [
        entities.StateStaffExternalId.new_with_defaults(
            state_code="US_XX",
            external_id="123",
            id_type="US_XX_STAFF",
        )
    ]
    staff.role_periods = [
        entities.StateStaffRolePeriod.new_with_defaults(
            state_code="US_XX",
            external_id="R1",
            start_date=datetime.date(2023, 1, 1),
            end_date=datetime.date(2023, 6, 1),
            role_type=StateStaffRoleType.SUPERVISION_OFFICER,
            role_type_raw_text="SUP_OF",
            role_subtype=StateStaffRoleSubtype.SUPERVISION_OFFICER,
            role_subtype_raw_text="SUP_OF",
        )
    ]
    staff.supervisor_periods = [
        entities.StateStaffSupervisorPeriod.new_with_defaults(
            state_code="US_XX",
            external_id="S1",
            start_date=datetime.date(2023, 1, 1),
            end_date=datetime.date(2023, 6, 1),
            supervisor_staff_external_id="S1",
            supervisor_staff_external_id_type="SUPERVISOR",
        )
    ]
    staff.location_periods = [
        entities.StateStaffLocationPeriod.new_with_defaults(
            state_code="US_XX",
            external_id="L1",
            start_date=datetime.date(2023, 1, 1),
            end_date=datetime.date(2023, 6, 1),
            location_external_id="L1",
        )
    ]
    staff.caseload_type_periods = [
        entities.StateStaffCaseloadTypePeriod.new_with_defaults(
            state_code="US_XX",
            external_id="C1",
            caseload_type=StateStaffCaseloadType.OTHER,
            caseload_type_raw_text="O",
            start_date=datetime.date(2023, 1, 1),
            end_date=datetime.date(2023, 6, 1),
        )
    ]

    if set_back_edges:
        set_backedges(staff, entities_module_context)

    if set_ids:
        for entity in get_all_entities_from_tree(staff, entities_module_context):
            if entity.get_id():
                raise ValueError(
                    f"Found entity [{entity}] with already set id field."
                    f"Not expected to be set until this part of the "
                    f"function."
                )
            id_name = entity.get_class_id_name()
            entity.set_field(id_name, id(entity))

    return staff


def generate_full_graph_normalized_state_person() -> normalized_entities.NormalizedStatePerson:
    """Test util for generating a NormalizedStatePerson that has at least one child of
    each possible Entity type, with all possible edge types defined between
    objects.
    """
    entities_module_context = entities_module_context_for_module(normalized_entities)
    assessment1 = normalized_entities.NormalizedStateAssessment(
        assessment_id=1,
        external_id="a1",
        assessment_class=StateAssessmentClass.RISK,
        assessment_class_raw_text=None,
        assessment_type=StateAssessmentType.LSIR,
        assessment_type_raw_text="LSIR",
        assessment_date=datetime.date(2003, month=8, day=10),
        state_code="US_XX",
        assessment_score=55,
        assessment_level=StateAssessmentLevel.MEDIUM,
        assessment_level_raw_text="MED",
        assessment_metadata="assessment metadata",
        conducting_staff_external_id="EMP3",
        conducting_staff_external_id_type="US_XX_STAFF_ID",
        conducting_staff_id=1,
        sequence_num=1,
    )

    assessment2 = normalized_entities.NormalizedStateAssessment(
        assessment_id=2,
        external_id="a2",
        assessment_class=StateAssessmentClass.RISK,
        assessment_class_raw_text=None,
        assessment_type=StateAssessmentType.LSIR,
        assessment_type_raw_text="LSIR",
        assessment_date=datetime.date(2004, month=9, day=10),
        state_code="US_XX",
        assessment_score=10,
        assessment_level=StateAssessmentLevel.LOW,
        assessment_level_raw_text="LOW",
        assessment_metadata="more assessment metadata",
        conducting_staff_external_id="EMP3",
        conducting_staff_external_id_type="US_XX_STAFF_ID",
        conducting_staff_id=1,
        sequence_num=2,
    )

    program_assignment = normalized_entities.NormalizedStateProgramAssignment(
        program_assignment_id=1,
        external_id="program_assignment_external_id_1",
        state_code="US_XX",
        participation_status=StateProgramAssignmentParticipationStatus.IN_PROGRESS,
        participation_status_raw_text="IN_PROGRESS",
        referral_date=datetime.date(year=2019, month=2, day=10),
        start_date=datetime.date(year=2019, month=2, day=11),
        program_id="program_id",
        program_location_id="program_location_id",
        referring_staff_external_id="EMP1",
        referring_staff_external_id_type="US_XX_STAFF_ID",
        referring_staff_id=1,
        sequence_num=1,
    )

    program_assignment2 = normalized_entities.NormalizedStateProgramAssignment(
        program_assignment_id=2,
        external_id="program_assignment_external_id_2",
        state_code="US_XX",
        participation_status=StateProgramAssignmentParticipationStatus.DISCHARGED_UNKNOWN,
        participation_status_raw_text="DISCHARGED_UNKNOWN",
        referral_date=datetime.date(year=2019, month=2, day=10),
        start_date=datetime.date(year=2019, month=2, day=11),
        discharge_date=datetime.date(year=2019, month=2, day=12),
        program_id="program_id",
        program_location_id="program_location_id",
        referring_staff_external_id="EMP1",
        referring_staff_external_id_type="US_XX_STAFF_ID",
        referring_staff_id=1,
        sequence_num=2,
    )

    incident_outcome = normalized_entities.NormalizedStateIncarcerationIncidentOutcome(
        incarceration_incident_outcome_id=1,
        external_id="io1",
        outcome_type=StateIncarcerationIncidentOutcomeType.WARNING,
        outcome_type_raw_text="WARNING",
        date_effective=datetime.date(year=2003, month=8, day=20),
        state_code="US_XX",
        outcome_description="LOSS OF COMMISSARY",
        punishment_length_days=30,
    )

    incarceration_incident = normalized_entities.NormalizedStateIncarcerationIncident(
        incarceration_incident_id=1,
        external_id="i1",
        incident_type=StateIncarcerationIncidentType.CONTRABAND,
        incident_type_raw_text="CONTRABAND",
        incident_date=datetime.date(year=2003, month=8, day=10),
        state_code="US_XX",
        facility="ALCATRAZ",
        location_within_facility="13B",
        incident_details="Found contraband cell phone.",
        incarceration_incident_outcomes=[incident_outcome],
    )

    supervision_violation = normalized_entities.NormalizedStateSupervisionViolation(
        supervision_violation_id=1,
        external_id="sv1",
        violation_date=datetime.date(year=2004, month=9, day=1),
        state_code="US_XX",
        is_violent=False,
        supervision_violation_types=[
            normalized_entities.NormalizedStateSupervisionViolationTypeEntry(
                supervision_violation_type_entry_id=1,
                state_code="US_XX",
                violation_type=StateSupervisionViolationType.TECHNICAL,
                violation_type_raw_text="TECHNICAL",
            ),
        ],
        supervision_violated_conditions=[
            normalized_entities.NormalizedStateSupervisionViolatedConditionEntry(
                supervision_violated_condition_entry_id=1,
                state_code="US_XX",
                condition=StateSupervisionViolatedConditionType.SPECIAL_CONDITIONS,
                condition_raw_text="MISSED CURFEW",
            )
        ],
    )

    supervision_violation_response = normalized_entities.NormalizedStateSupervisionViolationResponse(
        supervision_violation_response_id=1,
        external_id="svr1",
        response_type=StateSupervisionViolationResponseType.CITATION,
        response_date=datetime.date(year=2004, month=9, day=2),
        state_code="US_XX",
        deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.SUPERVISION_OFFICER,
        sequence_num=1,
    )

    supervision_violation.supervision_violation_responses = [
        supervision_violation_response
    ]

    violation_response_decision = (
        normalized_entities.NormalizedStateSupervisionViolationResponseDecisionEntry(
            supervision_violation_response_decision_entry_id=1,
            state_code="US_XX",
            decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
            decision_raw_text="PR",
        )
    )
    supervision_violation_response.supervision_violation_response_decisions = [
        violation_response_decision
    ]

    supervision_contact = normalized_entities.NormalizedStateSupervisionContact(
        supervision_contact_id=1,
        external_id="CONTACT_ID",
        status=StateSupervisionContactStatus.COMPLETED,
        status_raw_text="COMPLETED",
        contact_type=StateSupervisionContactType.DIRECT,
        contact_type_raw_text="FACE_TO_FACE",
        contact_date=datetime.date(year=1111, month=1, day=2),
        contact_datetime=datetime.datetime(1111, 1, 2, 0, 0, 0),
        state_code="US_XX",
        contact_reason=StateSupervisionContactReason.GENERAL_CONTACT,
        contact_reason_raw_text="GENERAL_CONTACT",
        location=StateSupervisionContactLocation.RESIDENCE,
        location_raw_text="RESIDENCE",
        verified_employment=True,
        resulted_in_arrest=False,
        contacting_staff_external_id="EMP2",
        contacting_staff_external_id_type="US_XX_STAFF_ID",
        contacting_staff_id=1,
    )

    scheduled_supervision_contact = (
        normalized_entities.NormalizedStateScheduledSupervisionContact(
            scheduled_supervision_contact_id=1,
            external_id="SCHEDULED_CONTACT_ID",
            status=StateScheduledSupervisionContactStatus.SCHEDULED,
            status_raw_text="SCHEDULED",
            contact_type=StateScheduledSupervisionContactType.DIRECT,
            contact_type_raw_text="FACE_TO_FACE",
            scheduled_contact_date=datetime.date(year=2020, month=1, day=2),
            scheduled_contact_datetime=datetime.datetime(2020, 1, 2, 0, 0, 0),
            update_datetime=datetime.datetime(2020, 1, 1),
            state_code="US_XX",
            contact_reason=StateScheduledSupervisionContactReason.GENERAL_CONTACT,
            contact_reason_raw_text="GENERAL_CONTACT",
            location=StateScheduledSupervisionContactLocation.RESIDENCE,
            location_raw_text="RESIDENCE",
            contact_method=StateScheduledSupervisionContactMethod.IN_PERSON,
            contact_method_raw_text="CONTACT",
            contacting_staff_external_id="EMP2",
            contacting_staff_external_id_type="US_XX_STAFF_ID",
            contacting_staff_id=1,
            sequence_num=1,
        )
    )

    incarceration_sentence = normalized_entities.NormalizedStateIncarcerationSentence(
        incarceration_sentence_id=1,
        external_id="BOOK_ID1234-1",
        status=StateSentenceStatus.COMPLETED,
        status_raw_text="COMPLETED",
        incarceration_type=StateIncarcerationType.STATE_PRISON,
        incarceration_type_raw_text="PRISON",
        date_imposed=datetime.date(year=2018, month=7, day=3),
        projected_min_release_date=datetime.date(year=2017, month=5, day=14),
        projected_max_release_date=None,
        parole_eligibility_date=datetime.date(year=2018, month=5, day=14),
        state_code="US_XX",
        county_code="US_XX_COUNTY",
        #   - What
        # These will be None if is_life is true
        min_length_days=90,
        max_length_days=900,
        is_life=False,
        is_capital_punishment=False,
        parole_possible=True,
        initial_time_served_days=None,
        good_time_days=10,
        earned_time_days=None,
    )

    supervision_sentence = normalized_entities.NormalizedStateSupervisionSentence(
        supervision_sentence_id=1,
        external_id="BOOK_ID1234-2",
        status=StateSentenceStatus.SERVING,
        status_raw_text="SERVING",
        supervision_type=StateSupervisionSentenceSupervisionType.PAROLE,
        supervision_type_raw_text="PAROLE",
        projected_completion_date=datetime.date(year=2020, month=5, day=14),
        completion_date=None,
        state_code="US_XX",
        min_length_days=None,
        max_length_days=200,
    )

    incarceration_period = normalized_entities.NormalizedStateIncarcerationPeriod(
        incarceration_period_id=1,
        external_id="ip1",
        incarceration_type=StateIncarcerationType.STATE_PRISON,
        incarceration_type_raw_text=None,
        admission_date=datetime.date(year=2003, month=8, day=1),
        release_date=datetime.date(year=2004, month=8, day=1),
        state_code="US_XX",
        county_code="US_XX_COUNTY",
        facility="ALCATRAZ",
        housing_unit="BLOCK A",
        admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        admission_reason_raw_text="NEW ADMISSION",
        release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        release_reason_raw_text="CONDITIONAL RELEASE",
        sequence_num=1,
    )

    charge = normalized_entities.NormalizedStateCharge(
        charge_id=1,
        external_id="CHARGE1_EXTERNAL_ID",
        status=StateChargeStatus.CONVICTED,
        status_raw_text="CONVICTED",
        offense_date=datetime.date(year=2003, month=7, day=1),
        date_charged=datetime.date(year=2003, month=8, day=1),
        state_code="US_XX",
        statute="A102.3",
        description="DRUG POSSESSION",
        attempted=True,
        classification_type=StateChargeClassificationType.FELONY,
        classification_type_raw_text="F",
        classification_subtype="A",
        counts=1,
        charge_notes=None,
    )

    charge2 = normalized_entities.NormalizedStateCharge(
        charge_id=2,
        external_id="CHARGE2_EXTERNAL_ID",
        status=StateChargeStatus.CONVICTED,
        status_raw_text="CONVICTED",
        offense_date=datetime.date(year=2003, month=7, day=1),
        date_charged=datetime.date(year=2003, month=8, day=1),
        state_code="US_XX",
        statute="A102.3",
        description="DRUG POSSESSION",
        attempted=True,
        classification_type=StateChargeClassificationType.FELONY,
        classification_type_raw_text="F",
        classification_subtype="B",
        counts=1,
        charge_notes=None,
    )

    charge3 = normalized_entities.NormalizedStateCharge(
        charge_id=3,
        external_id="CHARGE3_EXTERNAL_ID",
        status=StateChargeStatus.DROPPED,
        status_raw_text="DROPPED",
        offense_date=datetime.date(year=2003, month=7, day=1),
        date_charged=datetime.date(year=2003, month=8, day=1),
        state_code="US_XX",
        statute="A102.3",
        description="DRUG POSSESSION",
        attempted=True,
        classification_type=StateChargeClassificationType.FELONY,
        classification_type_raw_text="F",
        classification_subtype="AA",
        counts=1,
        charge_notes=None,
    )

    supervision_sentence.charges = [charge, charge2, charge3]
    incarceration_sentence.charges = [charge, charge2, charge3]

    early_discharge_1 = normalized_entities.NormalizedStateEarlyDischarge(
        early_discharge_id=1,
        external_id="ed1",
        request_date=datetime.date(year=2001, month=7, day=1),
        decision_date=datetime.date(year=2001, month=7, day=20),
        decision=StateEarlyDischargeDecision.SENTENCE_TERMINATION_GRANTED,
        decision_raw_text="approved",
        deciding_body_type=StateActingBodyType.PAROLE_BOARD,
        deciding_body_type_raw_text="pb",
        requesting_body_type=StateActingBodyType.SENTENCED_PERSON,
        requesting_body_type_raw_text="sentenced_person",
        state_code="US_XX",
        county_code="COUNTY",
    )
    early_discharge_2 = normalized_entities.NormalizedStateEarlyDischarge(
        early_discharge_id=2,
        external_id="ed2",
        request_date=datetime.date(year=2002, month=7, day=1),
        decision_date=datetime.date(year=2002, month=7, day=20),
        decision=StateEarlyDischargeDecision.UNSUPERVISED_PROBATION_GRANTED,
        decision_raw_text="conditionally_approved",
        deciding_body_type=StateActingBodyType.COURT,
        deciding_body_type_raw_text="c",
        requesting_body_type=StateActingBodyType.SENTENCED_PERSON,
        requesting_body_type_raw_text="sentenced_person",
        state_code="US_XX",
        county_code="COUNTY",
    )
    early_discharge_3 = normalized_entities.NormalizedStateEarlyDischarge(
        early_discharge_id=3,
        external_id="ed3",
        request_date=datetime.date(year=2001, month=1, day=1),
        decision_date=datetime.date(year=2001, month=1, day=20),
        decision=StateEarlyDischargeDecision.REQUEST_DENIED,
        decision_raw_text="denied",
        deciding_body_type=StateActingBodyType.PAROLE_BOARD,
        deciding_body_type_raw_text="pb",
        requesting_body_type=StateActingBodyType.SENTENCED_PERSON,
        requesting_body_type_raw_text="sentenced_person",
        state_code="US_XX",
        county_code="COUNTY",
    )
    supervision_sentence.early_discharges = [early_discharge_1, early_discharge_2]
    incarceration_sentence.early_discharges = [early_discharge_3]

    supervision_case_type_entry = (
        normalized_entities.NormalizedStateSupervisionCaseTypeEntry(
            supervision_case_type_entry_id=1,
            state_code="US_XX",
            case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
            case_type_raw_text="DOMESTIC_VIOLENCE",
        )
    )

    supervision_period = normalized_entities.NormalizedStateSupervisionPeriod(
        supervision_period_id=1,
        external_id="sp1",
        supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        supervision_type_raw_text="PAROLE",
        start_date=datetime.date(year=2004, month=8, day=1),
        termination_date=None,
        state_code="US_XX",
        admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
        admission_reason_raw_text="RELEASE",
        termination_reason=None,
        termination_reason_raw_text=None,
        supervision_level=StateSupervisionLevel.EXTERNAL_UNKNOWN,
        supervision_level_raw_text="UNKNOWN",
        conditions="10PM CURFEW",
        case_type_entries=[supervision_case_type_entry],
        supervising_officer_staff_external_id="EMP1",
        supervising_officer_staff_external_id_type="US_XX_STAFF_ID",
        supervising_officer_staff_id=1,
        sequence_num=1,
    )

    task_deadline = normalized_entities.NormalizedStateTaskDeadline(
        task_deadline_id=1,
        state_code="US_XX",
        task_type=StateTaskType.DISCHARGE_FROM_INCARCERATION,
        eligible_date=datetime.date(2020, 9, 11),
        update_datetime=datetime.datetime(2023, 2, 1, 11, 19),
        task_metadata='{"external_id": "00000001-111123-371006", "sentence_type": "INCARCERATION"}',
        sequence_num=1,
    )

    sentence = normalized_entities.NormalizedStateSentence(
        sentence_id=1,
        state_code="US_XX",
        external_id="SENTENCE_EXTERNAL_ID_1",
        sentence_group_external_id="SENTENCE_GROUP_EXTERNAL_ID_1",
        sentence_inferred_group_id=None,
        sentence_imposed_group_id=None,
        sentence_metadata='{"BS_CCI": "", "BS_CRQ": "0", "SENTENCE_FLAG": "SENTENCE: 120 DAY"}',
        sentencing_authority=StateSentencingAuthority.STATE,
        sentence_type=StateSentenceType.STATE_PRISON,
        imposed_date=datetime.date(year=1956, month=3, day=16),
        is_life=True,
        is_capital_punishment=True,
    )

    charge_v2 = normalized_entities.NormalizedStateChargeV2(
        charge_v2_id=1,
        state_code="US_XX",
        status=StateChargeV2Status.PRESENT_WITHOUT_INFO,
        external_id="CHARGE_V2_EXTERNAL_ID_1",
        county_code="US_MO_JACKSON",
        ncic_code="0904",
        statute="10021040",
        description="TC: MURDER 1ST - FIST",
        is_violent=True,
        classification_type=StateChargeV2ClassificationType.FELONY,
        classification_type_raw_text="F",
        classification_subtype="O",
        judicial_district_code="22",
    )
    sentence.charges = [charge_v2]

    sentence_status_snapshot = (
        normalized_entities.NormalizedStateSentenceStatusSnapshot(
            sentence_status_snapshot_id=1,
            state_code="US_XX",
            status=StateSentenceStatus.SERVING,
            status_update_datetime=datetime.datetime(2023, 1, 1),
            status_end_datetime=None,
            sequence_num=1,
        )
    )
    sentence.sentence_status_snapshots = [sentence_status_snapshot]

    sentence_length = normalized_entities.NormalizedStateSentenceLength(
        sentence_length_id=1,
        state_code="US_XX",
        length_update_datetime=datetime.datetime(2023, 1, 1),
        sequence_num=1,
    )
    sentence.sentence_lengths = [sentence_length]

    sentence_group = normalized_entities.NormalizedStateSentenceGroup(
        sentence_group_id=1,
        sentence_inferred_group_id=None,
        external_id=assert_type(sentence.sentence_group_external_id, str),
        state_code="US_XX",
    )
    sentence_inferred_group = normalized_entities.NormalizedStateSentenceInferredGroup(
        sentence_inferred_group_id=1,
        external_id=sentence.external_id,
        state_code="US_XX",
    )
    sentence_imposed_group = normalized_entities.NormalizedStateSentenceImposedGroup(
        sentence_imposed_group_id=1,
        external_id=sentence.external_id,
        state_code="US_XX",
        imposed_date=datetime.date(2022, 1, 1),
        sentencing_authority=StateSentencingAuthority.STATE,
        serving_start_date=datetime.date(2022, 1, 1),
        most_severe_charge_v2_id=1,
    )
    sentence_group_length = normalized_entities.NormalizedStateSentenceGroupLength(
        sentence_group_length_id=1,
        state_code="US_XX",
        group_update_datetime=datetime.datetime(2023, 1, 1),
        sequence_num=1,
    )
    sentence_group.sentence_group_lengths = [sentence_group_length]

    drug_screen = normalized_entities.NormalizedStateDrugScreen(
        drug_screen_id=1,
        state_code="US_XX",
        external_id="12356",
        drug_screen_date=datetime.date(2022, 5, 8),
        drug_screen_result=StateDrugScreenResult.NEGATIVE,
        drug_screen_result_raw_text="DRUN",
        sample_type=StateDrugScreenSampleType.BREATH,
        sample_type_raw_text="BREATH",
        drug_screen_metadata='{"DRUGTYPE": "METH"}',
    )

    employment_period = normalized_entities.NormalizedStateEmploymentPeriod(
        employment_period_id=1,
        state_code="US_XX",
        external_id="12356",
        start_date=datetime.date(2022, 5, 8),
        end_date=datetime.date(2022, 5, 10),
        last_verified_date=datetime.date(2022, 5, 1),
        employment_status=StateEmploymentPeriodEmploymentStatus.EMPLOYED_PART_TIME,
        employment_status_raw_text="PT",
        end_reason=StateEmploymentPeriodEndReason.QUIT,
        end_reason_raw_text="PERSONAL",
        employer_name="ACME, INC.",
        employer_address="123 FAKE ST, ANYTOWN, XX, 00000",
        job_title=None,
    )

    address_period = normalized_entities.NormalizedStatePersonAddressPeriod(
        person_address_period_id=1,
        state_code="US_XX",
        address_start_date=datetime.date(2020, 1, 1),
        address_end_date=datetime.date(2022, 1, 1),
        address_line_1="123 SANTA STREET",
        address_line_2="APT 4",
        address_city="NORTH POLE",
        address_zip="10000",
        address_county="GLACIER COUNTY",
        address_type=StatePersonAddressType.PHYSICAL_RESIDENCE,
        full_address="123 SANTA STREET\nAPT 4\nNORTH POLE 10000",
    )

    housing_status_period = (
        normalized_entities.NormalizedStatePersonHousingStatusPeriod(
            person_housing_status_period_id=1,
            state_code="US_XX",
            housing_status_start_date=datetime.date(year=2006, month=7, day=2),
            housing_status_end_date=datetime.date(year=2007, month=7, day=2),
            housing_status_type=StatePersonHousingStatusType.PERMANENT_RESIDENCE,
        )
    )

    person = normalized_entities.NormalizedStatePerson(
        person_id=1,
        state_code="US_XX",
        ethnicity=StateEthnicity.PRESENT_WITHOUT_INFO,
        external_ids=[
            normalized_entities.NormalizedStatePersonExternalId(
                person_external_id_id=1,
                state_code="US_XX",
                external_id="ELITE_ID_123",
                id_type=US_ND_ELITE,
                is_current_display_id_for_type=True,
                is_stable_id_for_type=True,
                id_active_from_datetime=datetime.datetime(2020, 1, 1),
                id_active_to_datetime=None,
            )
        ],
        aliases=[
            normalized_entities.NormalizedStatePersonAlias(
                person_alias_id=1,
                state_code="US_XX",
                full_name="Beyoncé Giselle Knowles",
            ),
            normalized_entities.NormalizedStatePersonAlias(
                person_alias_id=1,
                state_code="US_XX",
                full_name="Beyoncé Giselle Knowles-Carter",
            ),
        ],
        races=[
            normalized_entities.NormalizedStatePersonRace(
                person_race_id=1,
                state_code="US_XX",
                race=StateRace.ASIAN,
                race_raw_text="ASIAN",
            ),
            normalized_entities.NormalizedStatePersonRace(
                person_race_id=1,
                state_code="US_XX",
                race=StateRace.BLACK,
                race_raw_text="BLACK",
            ),
        ],
        assessments=[assessment1, assessment2],
        program_assignments=[program_assignment, program_assignment2],
        incarceration_incidents=[incarceration_incident],
        supervision_violations=[supervision_violation],
        supervision_contacts=[supervision_contact],
        scheduled_supervision_contacts=[scheduled_supervision_contact],
        incarceration_sentences=[incarceration_sentence],
        supervision_sentences=[supervision_sentence],
        incarceration_periods=[incarceration_period],
        task_deadlines=[task_deadline],
        sentences=[sentence],
        supervision_periods=[supervision_period],
        sentence_groups=[sentence_group],
        sentence_inferred_groups=[sentence_inferred_group],
        sentence_imposed_groups=[sentence_imposed_group],
        drug_screens=[drug_screen],
        employment_periods=[employment_period],
        address_periods=[address_period],
        housing_status_periods=[housing_status_period],
        staff_relationship_periods=[
            normalized_entities.NormalizedStatePersonStaffRelationshipPeriod(
                person_staff_relationship_period_id=123,
                state_code=StateCode.US_XX.value,
                relationship_start_date=datetime.date(2021, 1, 1),
                relationship_end_date_exclusive=datetime.date(2023, 1, 1),
                system_type=StateSystemType.INCARCERATION,
                system_type_raw_text="CM",
                relationship_type=StatePersonStaffRelationshipType.CASE_MANAGER,
                relationship_type_raw_text="CM",
                associated_staff_external_id="EMP2",
                associated_staff_external_id_type="US_XX_STAFF_ID",
                associated_staff_id=1,
                relationship_priority=1,
                location_external_id=None,
            )
        ],
    )

    set_backedges(person, entities_module_context)

    return person


def generate_full_graph_normalized_state_staff() -> normalized_entities.NormalizedStateStaff:
    """Test util for generating a NormalizedStateStaff that has at least one child of
    each possible Entity type, with all possible edge types defined between
    objects.
    """
    entities_module_context = entities_module_context_for_module(normalized_entities)
    staff = normalized_entities.NormalizedStateStaff(
        staff_id=1,
        state_code="US_XX",
        full_name="Staff Name",
        email="staff@staff.com",
        external_ids=[
            normalized_entities.NormalizedStateStaffExternalId(
                staff_external_id_id=2,
                state_code="US_XX",
                external_id="123",
                id_type="US_XX_STAFF",
            )
        ],
        role_periods=[
            normalized_entities.NormalizedStateStaffRolePeriod(
                staff_role_period_id=3,
                state_code="US_XX",
                external_id="R1",
                start_date=datetime.date(2023, 1, 1),
                end_date=datetime.date(2023, 6, 1),
                role_type=StateStaffRoleType.SUPERVISION_OFFICER,
                role_type_raw_text="SUP_OF",
                role_subtype=StateStaffRoleSubtype.SUPERVISION_OFFICER,
                role_subtype_raw_text="SUP_OF",
            )
        ],
        supervisor_periods=[
            normalized_entities.NormalizedStateStaffSupervisorPeriod(
                staff_supervisor_period_id=4,
                state_code="US_XX",
                external_id="S1",
                start_date=datetime.date(2023, 1, 1),
                end_date=datetime.date(2023, 6, 1),
                supervisor_staff_external_id="S1",
                supervisor_staff_external_id_type="SUPERVISOR",
                supervisor_staff_id=2,
            )
        ],
        location_periods=[
            normalized_entities.NormalizedStateStaffLocationPeriod(
                staff_location_period_id=5,
                state_code="US_XX",
                external_id="L1",
                start_date=datetime.date(2023, 1, 1),
                end_date=datetime.date(2023, 6, 1),
                location_external_id="L1",
            )
        ],
        caseload_type_periods=[
            normalized_entities.NormalizedStateStaffCaseloadTypePeriod(
                staff_caseload_type_period_id=5,
                state_code="US_XX",
                external_id="C1",
                caseload_type=StateStaffCaseloadType.OTHER,
                caseload_type_raw_text="O",
                start_date=datetime.date(2023, 1, 1),
                end_date=datetime.date(2023, 6, 1),
            )
        ],
    )

    set_backedges(staff, entities_module_context)

    return staff


class TestFullEntityGraph(unittest.TestCase):
    """Tests for the generate_full_graph* helpers above."""

    def test_full_entity_graph_coverage(self) -> None:
        """Tests that the generate_full_graph_state_person() and
        generate_full_graph_state_staff() functions cover all entities in the
        state/entities.py module
        """
        entities_module_context = entities_module_context_for_module(state_entities)
        expected_entity_classes = get_all_entity_classes_in_module(entities)

        found_entity_classes = {
            type(e)
            for re in [
                generate_full_graph_state_person(
                    set_back_edges=True, include_person_back_edges=True, set_ids=True
                ),
                generate_full_graph_state_staff(set_back_edges=True, set_ids=True),
            ]
            for e in get_all_entities_from_tree(
                assert_type(re, Entity), entities_module_context
            )
        }
        missing_in_entity_graph = expected_entity_classes - found_entity_classes
        if missing_in_entity_graph:
            raise ValueError(
                f"Found entities defined in state/entities.py which are not included "
                f"in one of the generate_full_graph_state* functions: "
                f"{[cls.__name__ for cls in missing_in_entity_graph]}"
            )

    def test_full_normalized_entity_graph_coverage(self) -> None:
        """Tests that the generate_full_graph_normalized_state_person() and
        generate_full_graph_normalized_state_staff() functions cover all entities in the
        state/normalized_entities.py module
        """
        entities_module_context = entities_module_context_for_module(
            normalized_entities
        )
        expected_entity_classes = get_all_entity_classes_in_module(normalized_entities)

        found_entity_classes = {
            type(e)
            for re in [
                generate_full_graph_normalized_state_person(),
                generate_full_graph_normalized_state_staff(),
            ]
            for e in get_all_entities_from_tree(
                assert_type(re, Entity), entities_module_context
            )
        }
        missing_in_entity_graph = expected_entity_classes - found_entity_classes
        if missing_in_entity_graph:
            raise ValueError(
                f"Found entities defined in state/entities.py which are not included "
                f"in one of the generate_full_graph_state* functions: "
                f"{[cls.__name__ for cls in missing_in_entity_graph]}"
            )
