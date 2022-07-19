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
"""Test utils for generating state CoreEntity/Entity classes."""

import datetime
from collections import defaultdict
from typing import Dict, List, Optional, Sequence, Type

from recidiviz.common.constants.state.external_id_types import US_ND_ELITE
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentClass,
    StateAssessmentLevel,
    StateAssessmentType,
)
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_charge import (
    StateChargeClassificationType,
    StateChargeStatus,
)
from recidiviz.common.constants.state.state_court_case import (
    StateCourtCaseStatus,
    StateCourtType,
)
from recidiviz.common.constants.state.state_early_discharge import (
    StateEarlyDischargeDecision,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_incident import (
    StateIncarcerationIncidentOutcomeType,
    StateIncarcerationIncidentType,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
)
from recidiviz.common.constants.state.state_person import StateEthnicity, StateRace
from recidiviz.common.constants.state.state_program_assignment import (
    StateProgramAssignmentParticipationStatus,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_shared_enums import StateActingBodyType
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
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecidingBodyType,
    StateSupervisionViolationResponseType,
)
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.core_entity import CoreEntity
from recidiviz.persistence.entity.entity_utils import (
    CoreEntityFieldIndex,
    EntityFieldType,
    get_entities_by_type,
    is_standalone_class,
    print_entity_tree,
)
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.entity.state.entities import (
    StateAgent,
    StateIncarcerationIncidentOutcome,
    StateProgramAssignment,
)


def clear_db_ids(
    db_entities: Sequence[CoreEntity],
    # Default arg caches across calls to this function
    field_index: CoreEntityFieldIndex = CoreEntityFieldIndex(),
):
    """Clears primary key fields off of all entities in all of the provided
    |db_entities| graphs.
    """
    for entity in db_entities:
        entity.clear_id()
        for field_name in field_index.get_fields_with_non_empty_values(
            entity, EntityFieldType.FORWARD_EDGE
        ):
            clear_db_ids(entity.get_field_as_list(field_name))


def assert_no_unexpected_entities_in_db(
    expected_entities: Sequence[DatabaseEntity],
    session: Session,
    # Default arg caches across calls to this function
    field_index: CoreEntityFieldIndex = CoreEntityFieldIndex(),
):
    """Counts all of the entities present in the |expected_entities| graph by
    type and ensures that the same number of entities exists in the DB for each
    type.
    """
    entity_counter: Dict[Type, List[DatabaseEntity]] = defaultdict(list)
    get_entities_by_type(expected_entities, field_index, entity_counter)
    for cls, entities_of_cls in entity_counter.items():
        # Standalone classes do not need to be attached to a person by design,
        # so it is valid if some standalone entities are not reachable from the
        # provided |expected_entities|
        if is_standalone_class(cls):
            continue

        expected_ids = set()
        for entity in entities_of_cls:
            expected_ids.add(entity.get_id())
        db_entities = session.query(cls).all()
        db_ids = set()
        for entity in db_entities:
            db_ids.add(entity.get_id())

        if expected_ids != db_ids:
            print("\n********** Entities from |found_persons| **********\n")
            for entity in sorted(entities_of_cls, key=lambda x: x.get_id()):
                print_entity_tree(entity, field_index=field_index)
            print("\n********** Entities from db **********\n")
            for entity in sorted(db_entities, key=lambda x: x.get_id()):
                print_entity_tree(entity, field_index=field_index)
            raise ValueError(
                f"For cls {cls.__name__}, found difference in primary keys from"
                f"expected entities and those of entities read from db.\n"
                f"Expected ids not present in db: "
                f"{str(expected_ids - db_ids)}\n"
                f"Db ids not present in expected entities: "
                f"{str(db_ids - expected_ids)}\n"
            )


def generate_full_graph_state_person(
    set_back_edges: bool,
    include_person_back_edges: Optional[bool] = True,
    set_ids: Optional[bool] = False,
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

    person.ethnicities = [
        entities.StatePersonEthnicity.new_with_defaults(
            state_code="US_XX",
            ethnicity=StateEthnicity.NOT_HISPANIC,
            ethnicity_raw_text="NOT HISPANIC",
        )
    ]

    assessment_agent = entities.StateAgent.new_with_defaults(
        agent_type=StateAgentType.SUPERVISION_OFFICER,
        state_code="US_XX",
        full_name="MR SIR",
    )

    assessment1 = entities.StateAssessment.new_with_defaults(
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
        conducting_agent=assessment_agent,
    )

    assessment2 = entities.StateAssessment.new_with_defaults(
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
        conducting_agent=assessment_agent,
    )

    person.assessments = [assessment1, assessment2]

    program_assignment_agent = StateAgent.new_with_defaults(
        agent_type=StateAgentType.SUPERVISION_OFFICER,
        state_code="US_XX",
        full_name='{"full_name": "AGENT PO"}',
    )

    program_assignment = StateProgramAssignment.new_with_defaults(
        external_id="program_assignment_external_id_1",
        state_code="US_XX",
        participation_status=StateProgramAssignmentParticipationStatus.IN_PROGRESS,
        participation_status_raw_text="IN_PROGRESS",
        referral_date=datetime.date(year=2019, month=2, day=10),
        start_date=datetime.date(year=2019, month=2, day=11),
        program_id="program_id",
        program_location_id="program_location_id",
        referring_agent=program_assignment_agent,
    )

    program_assignment2 = StateProgramAssignment.new_with_defaults(
        external_id="program_assignment_external_id_2",
        state_code="US_XX",
        participation_status=StateProgramAssignmentParticipationStatus.DISCHARGED,
        participation_status_raw_text="DISCHARGED",
        referral_date=datetime.date(year=2019, month=2, day=10),
        start_date=datetime.date(year=2019, month=2, day=11),
        discharge_date=datetime.date(year=2019, month=2, day=12),
        program_id="program_id",
        program_location_id="program_location_id",
        referring_agent=program_assignment_agent,
    )

    person.program_assignments = [program_assignment, program_assignment2]

    incident_responding_officer = entities.StateAgent.new_with_defaults(
        agent_type=StateAgentType.CORRECTIONAL_OFFICER,
        state_code="US_XX",
        full_name="MR SIR",
    )

    incident_outcome = entities.StateIncarcerationIncidentOutcome.new_with_defaults(
        outcome_type=StateIncarcerationIncidentOutcomeType.WARNING,
        outcome_type_raw_text="WARNING",
        date_effective=datetime.date(year=2003, month=8, day=20),
        state_code="US_XX",
        outcome_description="LOSS OF COMMISSARY",
        punishment_length_days=30,
    )

    incarceration_incident = entities.StateIncarcerationIncident.new_with_defaults(
        incident_type=StateIncarcerationIncidentType.CONTRABAND,
        incident_type_raw_text="CONTRABAND",
        incident_date=datetime.date(year=2003, month=8, day=10),
        state_code="US_XX",
        facility="ALCATRAZ",
        location_within_facility="13B",
        incident_details="Found contraband cell phone.",
        responding_officer=incident_responding_officer,
        incarceration_incident_outcomes=[incident_outcome],
    )

    person.incarceration_incidents = [incarceration_incident]

    supervision_violation = entities.StateSupervisionViolation.new_with_defaults(
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
                condition="MISSED CURFEW",
            )
        ],
    )

    person.supervision_violations = [supervision_violation]

    supervising_officer = entities.StateAgent.new_with_defaults(
        agent_type=StateAgentType.SUPERVISION_OFFICER,
        state_code="US_XX",
        full_name="MS MADAM",
    )

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
        contacted_agent=supervising_officer,
    )

    person.supervision_contacts = [supervision_contact]

    supervision_officer_agent = entities.StateAgent.new_with_defaults(
        agent_type=StateAgentType.SUPERVISION_OFFICER,
        state_code="US_XX",
        full_name="JOHN SMITH",
    )

    supervision_violation_response = entities.StateSupervisionViolationResponse.new_with_defaults(
        response_type=StateSupervisionViolationResponseType.CITATION,
        response_date=datetime.date(year=2004, month=9, day=2),
        state_code="US_XX",
        deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.SUPERVISION_OFFICER,
        decision_agents=[supervision_officer_agent],
    )

    supervision_violation.supervision_violation_responses = [
        supervision_violation_response
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
    )

    person.incarceration_periods = [incarceration_period]

    person_supervising_officer = entities.StateAgent.new_with_defaults(
        state_code="US_XX",
        external_id="SUPERVISING_OFFICER_ID",
        full_name="SUPERVISING OFFICER",
        agent_type=StateAgentType.PRESENT_WITHOUT_INFO,
    )
    person.supervising_officer = person_supervising_officer

    judge = entities.StateAgent.new_with_defaults(
        agent_type=StateAgentType.JUDGE,
        state_code="US_XX",
        full_name="JUDGE JUDY",
    )

    court_case = entities.StateCourtCase.new_with_defaults(
        external_id="CASEID456",
        status=StateCourtCaseStatus.EXTERNAL_UNKNOWN,
        date_convicted=datetime.date(year=2018, month=7, day=1),
        next_court_date=datetime.date(year=2019, month=7, day=1),
        state_code="US_XX",
        court_type=StateCourtType.PRESENT_WITHOUT_INFO,
        court_type_raw_text=None,
        judge=judge,
    )

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
        court_case=court_case,
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
        court_case=court_case,
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
        court_case=court_case,
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
        supervising_officer=supervising_officer,
        case_type_entries=[supervision_case_type_entry],
    )

    person.supervision_periods = [supervision_period]

    if set_back_edges:
        incarceration_sentence_children: Sequence[Entity] = (
            *incarceration_sentence.charges,
            *incarceration_sentence.early_discharges,
        )

        for child in incarceration_sentence_children:
            if hasattr(child, "incarceration_sentences"):
                child.incarceration_sentences = [incarceration_sentence]  # type: ignore[attr-defined]
            else:
                child.incarceration_sentence = incarceration_sentence  # type: ignore[attr-defined]

        supervision_sentence_children: Sequence[Entity] = (
            *supervision_sentence.charges,
            *supervision_sentence.early_discharges,
        )

        for child in supervision_sentence_children:
            if hasattr(child, "supervision_sentences"):
                child.supervision_sentences = [supervision_sentence]  # type: ignore[attr-defined]
            else:
                child.supervision_sentence = supervision_sentence  # type: ignore[attr-defined]

        court_case.charges = [charge, charge2, charge3]

        incarceration_incident_children: List[
            StateIncarcerationIncidentOutcome
        ] = incarceration_incident.incarceration_incident_outcomes

        for child in incarceration_incident_children:
            child.incarceration_incident = incarceration_incident

        supervision_period_children: Sequence[Entity] = (
            *supervision_period.case_type_entries,
        )
        for child in supervision_period_children:
            if hasattr(child, "supervision_periods"):
                child.supervision_periods = [supervision_period]  # type: ignore[attr-defined]
            else:
                child.supervision_period = supervision_period  # type: ignore[attr-defined]

        supervision_violation_response.supervision_violation = supervision_violation

        for violation_type in supervision_violation.supervision_violation_types:
            violation_type.supervision_violation = supervision_violation

        for violated_condition in supervision_violation.supervision_violated_conditions:
            violated_condition.supervision_violation = supervision_violation

    all_entities: Sequence[Entity] = (
        *[person],
        *person.external_ids,
        *person.races,
        *person.aliases,
        *person.ethnicities,
        *person.assessments,
        *person.program_assignments,
        *person.incarceration_incidents,
        *person.supervision_violations,
        *person.supervision_contacts,
        *person.incarceration_sentences,
        *person.supervision_sentences,
        *person.incarceration_periods,
        *person.supervision_periods,
        *incarceration_sentence.charges,
        *incarceration_sentence.early_discharges,
        *supervision_sentence.early_discharges,
        *[court_case],
        *incarceration_incident.incarceration_incident_outcomes,
        *supervision_period.case_type_entries,
        *supervision_violation.supervision_violation_responses,
        *supervision_violation.supervision_violation_types,
        *supervision_violation.supervision_violated_conditions,
    )

    if include_person_back_edges and set_back_edges:
        if include_person_back_edges:
            for entity in all_entities:
                if isinstance(entity, entities.StatePerson):
                    continue

                entity.set_field("person", person)

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
