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

"""Represents data scraped for a single individual.
    TODO(#8905): Delete this whole file once v2 mappings migration is complete.
"""
from abc import abstractmethod
from typing import List, Optional

from recidiviz.common.str_field_utils import to_snake_case

PLURALS = {
    "state_person": "state_people",
    "state_person_race": "state_person_races",
    "state_person_ethnicity": "state_person_ethnicities",
    "state_alias": "state_aliases",
    "state_assessment": "state_assessments",
    "state_program_assignment": "state_program_assignments",
    "state_person_external_id": "state_person_external_ids",
    "state_supervision_sentence": "state_supervision_sentences",
    "state_incarceration_sentence": "state_incarceration_sentences",
    "state_supervision_period": "state_supervision_periods",
    "state_incarceration_period": "state_incarceration_periods",
    "state_charge": "state_charges",
    "state_incarceration_incident": "state_incarceration_incidents",
    "state_supervision_violation": "state_supervision_violations",
    "state_supervision_violation_response": "state_supervision_violation_responses",
}


class IngestObject:
    """Abstract base class for all the objects contained by IngestInfo"""

    def __eq__(self, other):
        return eq(self, other)

    def __lt__(self, other):
        return str(self) < str(other)

    def __bool__(self):
        return to_bool(self)

    def __str__(self):
        return to_string(self)

    def __repr__(self):
        return to_repr(self)

    @abstractmethod
    def __setattr__(self, key, value):
        """Implement using restricted_setattr"""

    def class_name(self) -> str:
        return to_snake_case(self.__class__.__name__)


class IngestInfo(IngestObject):
    """Class for information about multiple people."""

    def __init__(self, state_people=None):
        self.state_people: List[StatePerson] = state_people or []

    def __eq__(self, other):
        return eq(self, other, exclude=["_state_people_by_id"])

    def __bool__(self):
        return to_bool(self, exclude=["_state_people_by_id"])

    def __str__(self):
        return to_string(self, exclude=["_state_people_by_id"])

    def __repr__(self):
        return to_repr(self, exclude=["_state_people_by_id"])

    def __setattr__(self, name, value):
        restricted_setattr(self, "_state_people_by_id", name, value)

    def create_state_person(self, **kwargs) -> "StatePerson":
        person = StatePerson(**kwargs)
        self.state_people.append(person)
        return person

    def get_recent_state_person(self) -> Optional["StatePerson"]:
        if self.state_people:
            return self.state_people[-1]
        return None

    def get_state_person_by_id(self, state_person_id) -> Optional["StatePerson"]:
        return next(
            (sp for sp in self.state_people if sp.state_person_id == state_person_id),
            None,
        )

    def prune(self) -> "IngestInfo":
        self.state_people = [person.prune() for person in self.state_people if person]
        return self

    def sort(self):
        for person in self.state_people:
            person.sort()
        self.state_people.sort()

    def get_all_state_people(self, predicate=lambda _: True) -> List["StatePerson"]:
        return [person for person in self.state_people if predicate(person)]


class StatePerson(IngestObject):
    """Class for information about a person at the state level. Referenced from IngestInfo."""

    def __init__(
        self,
        state_person_id=None,
        full_name=None,
        surname=None,
        given_names=None,
        middle_names=None,
        name_suffix=None,
        birthdate=None,
        gender=None,
        age=None,
        current_address=None,
        residency_status=None,
        state_person_races=None,
        state_person_ethnicities=None,
        state_aliases=None,
        state_person_external_ids=None,
        state_assessments=None,
        state_supervision_sentences=None,
        state_incarceration_sentences=None,
        state_incarceration_periods=None,
        state_supervision_periods=None,
        state_program_assignments=None,
        state_incarceration_incidents=None,
        state_supervision_violations=None,
        state_supervision_contacts=None,
        supervising_officer=None,
        state_code=None,
    ):
        self.state_person_id: Optional[str] = state_person_id
        self.full_name: Optional[str] = full_name
        self.surname: Optional[str] = surname
        self.given_names: Optional[str] = given_names
        self.middle_names: Optional[str] = middle_names
        self.name_suffix: Optional[str] = name_suffix
        self.birthdate: Optional[str] = birthdate
        self.gender: Optional[str] = gender
        self.age: Optional[str] = age
        self.current_address: Optional[str] = current_address
        self.residency_status: Optional[str] = residency_status
        self.state_code: Optional[str] = state_code

        self.state_person_races: List[StatePersonRace] = state_person_races or []
        self.state_person_ethnicities: List[StatePersonEthnicity] = (
            state_person_ethnicities or []
        )
        self.state_aliases: List[StateAlias] = state_aliases or []
        self.state_person_external_ids: List[StatePersonExternalId] = (
            state_person_external_ids or []
        )
        self.state_assessments: List[StateAssessment] = state_assessments or []
        self.state_supervision_sentences: List[StateSupervisionSentence] = (
            state_supervision_sentences or []
        )
        self.state_incarceration_sentences: List[StateIncarcerationSentence] = (
            state_incarceration_sentences or []
        )
        self.state_incarceration_periods: List[StateIncarcerationPeriod] = (
            state_incarceration_periods or []
        )
        self.state_supervision_periods: List[StateSupervisionPeriod] = (
            state_supervision_periods or []
        )
        self.state_program_assignments: List[StateProgramAssignment] = (
            state_program_assignments or []
        )
        self.state_incarceration_incidents: List[StateIncarcerationIncident] = (
            state_incarceration_incidents or []
        )
        self.state_supervision_violations: List[StateSupervisionViolation] = (
            state_supervision_violations or []
        )
        self.state_supervision_contacts: List[StateSupervisionContact] = (
            state_supervision_contacts or []
        )
        self.supervising_officer: Optional[StateAgent] = supervising_officer

    def __setattr__(self, name, value):
        restricted_setattr(self, "supervising_officer", name, value)

    def create_state_person_race(self, **kwargs) -> "StatePersonRace":
        race = StatePersonRace(**kwargs)
        self.state_person_races.append(race)
        return race

    def create_state_person_ethnicity(self, **kwargs) -> "StatePersonEthnicity":
        ethnicity = StatePersonEthnicity(**kwargs)
        self.state_person_ethnicities.append(ethnicity)
        return ethnicity

    def create_state_alias(self, **kwargs) -> "StateAlias":
        alias = StateAlias(**kwargs)
        self.state_aliases.append(alias)
        return alias

    def create_state_person_external_id(self, **kwargs) -> "StatePersonExternalId":
        external_id = StatePersonExternalId(**kwargs)
        self.state_person_external_ids.append(external_id)
        return external_id

    def create_state_assessment(self, **kwargs) -> "StateAssessment":
        assessment = StateAssessment(**kwargs)
        self.state_assessments.append(assessment)
        return assessment

    def create_state_supervision_sentence(self, **kwargs) -> "StateSupervisionSentence":
        supervision_sentence = StateSupervisionSentence(**kwargs)
        self.state_supervision_sentences.append(supervision_sentence)
        return supervision_sentence

    def create_state_incarceration_sentence(
        self, **kwargs
    ) -> "StateIncarcerationSentence":
        incarceration_sentence = StateIncarcerationSentence(**kwargs)
        self.state_incarceration_sentences.append(incarceration_sentence)
        return incarceration_sentence

    def create_state_incarceration_period(self, **kwargs) -> "StateIncarcerationPeriod":
        incarceration_period = StateIncarcerationPeriod(**kwargs)
        self.state_incarceration_periods.append(incarceration_period)
        return incarceration_period

    def create_state_supervision_period(self, **kwargs) -> "StateSupervisionPeriod":
        supervision_period = StateSupervisionPeriod(**kwargs)
        self.state_supervision_periods.append(supervision_period)
        return supervision_period

    def create_state_program_assignment(self, **kwargs) -> "StateProgramAssignment":
        program_assignment = StateProgramAssignment(**kwargs)
        self.state_program_assignments.append(program_assignment)
        return program_assignment

    def create_state_incarceration_incident(
        self, **kwargs
    ) -> "StateIncarcerationIncident":
        incarceration_incident = StateIncarcerationIncident(**kwargs)
        self.state_incarceration_incidents.append(incarceration_incident)
        return incarceration_incident

    def create_state_supervision_violation(
        self, **kwargs
    ) -> "StateSupervisionViolation":
        supervision_violation = StateSupervisionViolation(**kwargs)
        self.state_supervision_violations.append(supervision_violation)
        return supervision_violation

    def create_state_supervision_contact(self, **kwargs) -> "StateSupervisionContact":
        contact = StateSupervisionContact(**kwargs)
        self.state_supervision_contacts.append(contact)
        return contact

    def create_state_agent(self, **kwargs) -> "StateAgent":
        self.supervising_officer = StateAgent(**kwargs)
        return self.supervising_officer

    def get_state_person_race_by_id(
        self, state_person_race_id
    ) -> Optional["StatePersonRace"]:
        return next(
            (
                spr
                for spr in self.state_person_races
                if spr.state_person_race_id == state_person_race_id
            ),
            None,
        )

    def get_state_person_ethnicity_by_id(
        self, state_person_ethnicity_id
    ) -> Optional["StatePersonEthnicity"]:
        return next(
            (
                spe
                for spe in self.state_person_ethnicities
                if spe.state_person_ethnicity_id == state_person_ethnicity_id
            ),
            None,
        )

    def get_state_alias_by_id(self, state_alias_id) -> Optional["StateAlias"]:
        return next(
            (sa for sa in self.state_aliases if sa.state_alias_id == state_alias_id),
            None,
        )

    def get_state_person_external_id_by_id(
        self, state_person_external_id_id
    ) -> Optional["StatePersonExternalId"]:
        return next(
            (
                eid
                for eid in self.state_person_external_ids
                if eid.state_person_external_id_id == state_person_external_id_id
            ),
            None,
        )

    def get_state_assessment_by_id(
        self, state_assessment_id
    ) -> Optional["StateAssessment"]:
        return next(
            (
                sa
                for sa in self.state_assessments
                if sa.state_assessment_id == state_assessment_id
            ),
            None,
        )

    def get_state_supervision_sentence_by_id(
        self, supervision_sentence_id
    ) -> Optional["StateSupervisionSentence"]:
        return next(
            (
                ss
                for ss in self.state_supervision_sentences
                if ss.state_supervision_sentence_id == supervision_sentence_id
            ),
            None,
        )

    def get_state_incarceration_sentence_by_id(
        self, incarceration_sentence_id
    ) -> Optional["StateIncarcerationSentence"]:
        return next(
            (
                ins
                for ins in self.state_incarceration_sentences
                if ins.state_incarceration_sentence_id == incarceration_sentence_id
            ),
            None,
        )

    def get_state_incarceration_period_by_id(
        self, incarceration_period_id
    ) -> Optional["StateIncarcerationPeriod"]:
        return next(
            (
                ip
                for ip in self.state_incarceration_periods
                if ip.state_incarceration_period_id == incarceration_period_id
            ),
            None,
        )

    def get_state_supervision_period_by_id(
        self, state_supervision_period_id
    ) -> Optional["StateSupervisionPeriod"]:
        return next(
            (
                sc
                for sc in self.state_supervision_periods
                if sc.state_supervision_period_id == state_supervision_period_id
            ),
            None,
        )

    def get_state_program_assignment_by_id(
        self, state_program_assignment_id
    ) -> Optional["StateProgramAssignment"]:
        return next(
            (
                pa
                for pa in self.state_program_assignments
                if pa.state_program_assignment_id == state_program_assignment_id
            ),
            None,
        )

    def get_state_incarceration_incident_by_id(
        self, state_incarceration_incident_id
    ) -> Optional["StateIncarcerationIncident"]:
        return next(
            (
                ii
                for ii in self.state_incarceration_incidents
                if ii.state_incarceration_incident_id == state_incarceration_incident_id
            ),
            None,
        )

    def get_state_supervision_violation_by_id(
        self, state_supervision_violation_id
    ) -> Optional["StateSupervisionViolation"]:
        return next(
            (
                sc
                for sc in self.state_supervision_violations
                if sc.state_supervision_violation_id == state_supervision_violation_id
            ),
            None,
        )

    def get_state_supervision_contact_by_id(
        self, state_supervision_contact_id
    ) -> Optional["StateSupervisionContact"]:
        return next(
            (
                sc
                for sc in self.state_supervision_contacts
                if sc.state_supervision_contact_id == state_supervision_contact_id
            ),
            None,
        )

    def prune(self) -> "StatePerson":
        """Prune all children from StatePerson."""
        self.state_supervision_sentences = [
            ss.prune() for ss in self.state_supervision_sentences if ss
        ]
        self.state_incarceration_sentences = [
            ins.prune() for ins in self.state_incarceration_sentences if ins
        ]
        self.state_supervision_periods = [
            sp.prune() for sp in self.state_supervision_periods if sp
        ]
        self.state_assessments = [a.prune() for a in self.state_assessments if a]
        self.state_program_assignments = [
            p for p in self.state_program_assignments if p
        ]
        self.state_incarceration_incidents = [
            ii.prune() for ii in self.state_incarceration_incidents if ii
        ]
        self.state_supervision_violations = [
            sv for sv in self.state_supervision_violations if sv
        ]
        self.state_supervision_contacts = [
            c.prune() for c in self.state_supervision_contacts if c
        ]
        if not self.supervising_officer:
            self.supervising_officer = None
        return self

    def sort(self):
        self.state_person_races.sort()
        self.state_person_ethnicities.sort()
        self.state_aliases.sort()
        self.state_person_external_ids.sort()
        self.state_assessments.sort()
        self.state_program_assignments.sort()
        self.state_incarceration_incidents.sort()
        self.state_supervision_violations.sort()
        self.state_supervision_contacts.sort()

        for supervision_sentence in self.state_supervision_sentences:
            supervision_sentence.sort()
        self.state_supervision_sentences.sort()

        for incarceration_sentence in self.state_incarceration_sentences:
            incarceration_sentence.sort()
        self.state_incarceration_sentences.sort()

        for incarceration_period in self.state_incarceration_periods:
            incarceration_period.sort()
        self.state_incarceration_periods.sort()

        for supervision_period in self.state_supervision_periods:
            supervision_period.sort()
        self.state_supervision_periods.sort()


class StatePersonExternalId(IngestObject):
    """Class for information about one of a StatePerson's external ids. Referenced from StatePerson."""

    def __init__(self, state_person_external_id_id=None, id_type=None, state_code=None):
        self.state_person_external_id_id: Optional[str] = state_person_external_id_id
        self.id_type: Optional[str] = id_type
        self.state_code: Optional[str] = state_code

    def __setattr__(self, name, value):
        restricted_setattr(self, "state_code", name, value)


class StateAlias(IngestObject):
    """Class for information about an alias. Referenced from StatePerson."""

    def __init__(
        self,
        state_alias_id=None,
        state_code=None,
        full_name=None,
        surname=None,
        given_names=None,
        middle_names=None,
        name_suffix=None,
        alias_type=None,
    ):
        self.state_alias_id: Optional[str] = state_alias_id
        self.state_code: Optional[str] = state_code
        self.full_name: Optional[str] = full_name
        self.surname: Optional[str] = surname
        self.given_names: Optional[str] = given_names
        self.middle_names: Optional[str] = middle_names
        self.name_suffix: Optional[str] = name_suffix
        self.alias_type: Optional[str] = alias_type

    def __setattr__(self, name, value):
        restricted_setattr(self, "alias_type", name, value)


class StatePersonRace(IngestObject):
    """Class for information about a state person's race. Referenced from StatePerson."""

    def __init__(self, state_person_race_id=None, race=None, state_code=None):
        self.state_person_race_id: Optional[str] = state_person_race_id
        self.race: Optional[str] = race
        self.state_code: Optional[str] = state_code

    def __setattr__(self, name, value):
        restricted_setattr(self, "state_code", name, value)


class StatePersonEthnicity(IngestObject):
    """Class for information about a state person's ethnicity. Referenced from StatePerson."""

    def __init__(self, state_person_ethnicity_id=None, ethnicity=None, state_code=None):
        self.state_person_ethnicity_id: Optional[str] = state_person_ethnicity_id
        self.ethnicity: Optional[str] = ethnicity
        self.state_code: Optional[str] = state_code

    def __setattr__(self, name, value):
        restricted_setattr(self, "state_code", name, value)


class StateAssessment(IngestObject):
    """Class for information about an assessment. Referenced from StatePerson."""

    def __init__(
        self,
        state_assessment_id=None,
        assessment_class=None,
        assessment_type=None,
        assessment_date=None,
        state_code=None,
        assessment_score=None,
        assessment_level=None,
        assessment_metadata=None,
        conducting_agent=None,
    ):
        self.state_assessment_id: Optional[str] = state_assessment_id
        self.assessment_class: Optional[str] = assessment_class
        self.assessment_type: Optional[str] = assessment_type
        self.assessment_date: Optional[str] = assessment_date
        self.state_code: Optional[str] = state_code
        self.assessment_score: Optional[str] = assessment_score
        self.assessment_level: Optional[str] = assessment_level
        self.assessment_metadata: Optional[str] = assessment_metadata

        self.conducting_agent: Optional[StateAgent] = conducting_agent

    def __setattr__(self, name, value):
        restricted_setattr(self, "conducting_agent", name, value)

    def create_state_agent(self, **kwargs) -> "StateAgent":
        self.conducting_agent = StateAgent(**kwargs)
        return self.conducting_agent

    def prune(self) -> "StateAssessment":
        if not self.conducting_agent:
            self.conducting_agent = None
        return self


class StateSupervisionSentence(IngestObject):
    """Class for information about a sentence to supervision."""

    def __init__(
        self,
        state_supervision_sentence_id=None,
        status=None,
        supervision_type=None,
        date_imposed=None,
        start_date=None,
        projected_completion_date=None,
        completion_date=None,
        state_code=None,
        county_code=None,
        min_length=None,
        max_length=None,
        state_charges=None,
        state_early_discharges=None,
    ):
        self.state_supervision_sentence_id: Optional[
            str
        ] = state_supervision_sentence_id
        self.status: Optional[str] = status
        self.supervision_type: Optional[str] = supervision_type
        self.date_imposed: Optional[str] = date_imposed
        self.start_date: Optional[str] = start_date
        self.projected_completion_date: Optional[str] = projected_completion_date
        self.completion_date: Optional[str] = completion_date
        self.state_code: Optional[str] = state_code
        self.county_code: Optional[str] = county_code
        self.min_length: Optional[str] = min_length
        self.max_length: Optional[str] = max_length

        self.state_charges: List[StateCharge] = state_charges or []
        self.state_early_discharges: List[StateEarlyDischarge] = (
            state_early_discharges or []
        )

    def __setattr__(self, name, value):
        restricted_setattr(self, "state_early_discharges", name, value)

    def create_state_charge(self, **kwargs) -> "StateCharge":
        charge = StateCharge(**kwargs)
        self.state_charges.append(charge)
        return charge

    def create_state_early_discharge(self, **kwargs) -> "StateEarlyDischarge":
        early_discharge = StateEarlyDischarge(**kwargs)
        self.state_early_discharges.append(early_discharge)
        return early_discharge

    def get_state_charge_by_id(self, state_charge_id) -> Optional["StateCharge"]:
        return next(
            (sc for sc in self.state_charges if sc.state_charge_id == state_charge_id),
            None,
        )

    def get_state_early_discharge_by_id(
        self, state_early_discharge_id
    ) -> Optional["StateEarlyDischarge"]:
        return next(
            (
                ed
                for ed in self.state_early_discharges
                if ed.state_early_discharge_id == state_early_discharge_id
            ),
            None,
        )

    def prune(self) -> "StateSupervisionSentence":
        self.state_charges = [sc.prune() for sc in self.state_charges if sc]

        return self

    def sort(self):
        self.state_charges.sort()


class StateIncarcerationSentence(IngestObject):
    """Class for information about a sentence to incarceration."""

    def __init__(
        self,
        state_incarceration_sentence_id=None,
        status=None,
        incarceration_type=None,
        date_imposed=None,
        start_date=None,
        projected_min_release_date=None,
        projected_max_release_date=None,
        completion_date=None,
        parole_eligibility_date=None,
        state_code=None,
        county_code=None,
        min_length=None,
        max_length=None,
        is_life=None,
        is_capital_punishment=None,
        parole_possible=None,
        initial_time_served=None,
        good_time=None,
        earned_time=None,
        state_charges=None,
        state_early_discharges=None,
    ):
        self.state_incarceration_sentence_id: Optional[
            str
        ] = state_incarceration_sentence_id
        self.status: Optional[str] = status
        self.incarceration_type: Optional[str] = incarceration_type
        self.date_imposed: Optional[str] = date_imposed
        self.start_date: Optional[str] = start_date
        self.projected_min_release_date: Optional[str] = projected_min_release_date
        self.projected_max_release_date: Optional[str] = projected_max_release_date
        self.completion_date: Optional[str] = completion_date
        self.parole_eligibility_date: Optional[str] = parole_eligibility_date
        self.state_code: Optional[str] = state_code
        self.county_code: Optional[str] = county_code
        self.min_length: Optional[str] = min_length
        self.max_length: Optional[str] = max_length
        self.is_life: Optional[str] = is_life
        self.is_capital_punishment: Optional[str] = is_capital_punishment
        self.parole_possible: Optional[str] = parole_possible
        self.initial_time_served: Optional[str] = initial_time_served
        self.good_time: Optional[str] = good_time
        self.earned_time: Optional[str] = earned_time

        self.state_charges: List[StateCharge] = state_charges or []
        self.state_early_discharges: List[StateEarlyDischarge] = (
            state_early_discharges or []
        )

    def __setattr__(self, name, value):
        restricted_setattr(self, "state_early_discharges", name, value)

    def create_state_charge(self, **kwargs) -> "StateCharge":
        state_charge = StateCharge(**kwargs)
        self.state_charges.append(state_charge)
        return state_charge

    def create_state_early_discharge(self, **kwargs) -> "StateEarlyDischarge":
        early_discharge = StateEarlyDischarge(**kwargs)
        self.state_early_discharges.append(early_discharge)
        return early_discharge

    def get_state_charge_by_id(self, state_charge_id) -> Optional["StateCharge"]:
        return next(
            (sc for sc in self.state_charges if sc.state_charge_id == state_charge_id),
            None,
        )

    def get_state_early_discharge_by_id(
        self, state_early_discharge_id
    ) -> Optional["StateEarlyDischarge"]:
        return next(
            (
                ed
                for ed in self.state_early_discharges
                if ed.state_early_discharge_id == state_early_discharge_id
            ),
            None,
        )

    def prune(self) -> "StateIncarcerationSentence":
        self.state_charges = [sc.prune() for sc in self.state_charges if sc]

        return self

    def sort(self):
        self.state_charges.sort()


class StateCharge(IngestObject):
    """Class for information about a charge. Referenced from IncarcerationSentence, SupervisionSentence, and Fine."""

    def __init__(
        self,
        state_charge_id=None,
        status=None,
        offense_date=None,
        date_charged=None,
        state_code=None,
        county_code=None,
        ncic_code=None,
        statute=None,
        description=None,
        attempted=None,
        classification_type=None,
        classification_subtype=None,
        offense_type=None,
        is_violent=None,
        is_sex_offense=None,
        counts=None,
        charge_notes=None,
        is_controlling=None,
        charging_entity=None,
        state_court_case=None,
    ):
        self.state_charge_id: Optional[str] = state_charge_id
        self.status: Optional[str] = status
        self.offense_date: Optional[str] = offense_date
        self.date_charged: Optional[str] = date_charged
        self.state_code: Optional[str] = state_code
        self.county_code: Optional[str] = county_code
        self.ncic_code: Optional[str] = ncic_code
        self.statute: Optional[str] = statute
        self.description: Optional[str] = description
        self.attempted: Optional[str] = attempted
        self.classification_type: Optional[str] = classification_type
        self.classification_subtype: Optional[str] = classification_subtype
        self.offense_type: Optional[str] = offense_type
        self.is_violent: Optional[str] = is_violent
        self.is_sex_offense: Optional[str] = is_sex_offense
        self.counts: Optional[str] = counts
        self.charge_notes: Optional[str] = charge_notes
        self.is_controlling: Optional[str] = is_controlling
        self.charging_entity: Optional[str] = charging_entity

        self.state_court_case: Optional[StateCourtCase] = state_court_case

    def __setattr__(self, name, value):
        restricted_setattr(self, "state_court_case", name, value)

    def create_state_court_case(self, **kwargs) -> "StateCourtCase":
        court_case = StateCourtCase(**kwargs)
        self.state_court_case = court_case
        return self.state_court_case

    def get_state_court_case_by_id(self, court_case_id) -> Optional["StateCourtCase"]:
        if (
            self.state_court_case
            and self.state_court_case.state_court_case_id == court_case_id
        ):
            return self.state_court_case
        return None

    def prune(self) -> "StateCharge":
        if not self.state_court_case:
            self.state_court_case = None
        return self


class StateCourtCase(IngestObject):
    """Class for information about a court case. Referenced from StateCharge."""

    def __init__(
        self,
        state_court_case_id=None,
        status=None,
        court_type=None,
        date_convicted=None,
        next_court_date=None,
        state_code=None,
        county_code=None,
        judicial_district_code=None,
        judge=None,
    ):
        self.state_court_case_id: Optional[str] = state_court_case_id
        self.status: Optional[str] = status
        self.court_type: Optional[str] = court_type
        self.date_convicted: Optional[str] = date_convicted
        self.next_court_date: Optional[str] = next_court_date
        self.state_code: Optional[str] = state_code
        self.county_code: Optional[str] = county_code
        self.judicial_district_code: Optional[str] = judicial_district_code

        self.judge: Optional[StateAgent] = judge

    def create_state_agent(self, **kwargs) -> "StateAgent":
        self.judge = StateAgent(**kwargs)
        return self.judge

    def get_state_agent_by_id(self, state_agent_id) -> Optional["StateAgent"]:
        if self.judge and self.judge.state_agent_id == state_agent_id:
            return self.judge
        return None

    def __setattr__(self, name, value):
        restricted_setattr(self, "judge", name, value)


class StateIncarcerationPeriod(IngestObject):
    """Class for information about a period of incarceration."""

    def __init__(
        self,
        state_incarceration_period_id=None,
        incarceration_type=None,
        admission_date=None,
        release_date=None,
        state_code=None,
        county_code=None,
        facility=None,
        housing_unit=None,
        admission_reason=None,
        release_reason=None,
        specialized_purpose_for_incarceration=None,
        custodial_authority=None,
    ):
        self.state_incarceration_period_id: Optional[
            str
        ] = state_incarceration_period_id
        self.incarceration_type: Optional[str] = incarceration_type
        self.admission_date: Optional[str] = admission_date
        self.release_date: Optional[str] = release_date
        self.state_code: Optional[str] = state_code
        self.county_code: Optional[str] = county_code
        self.facility: Optional[str] = facility
        self.housing_unit: Optional[str] = housing_unit
        self.admission_reason: Optional[str] = admission_reason
        self.release_reason: Optional[str] = release_reason
        self.specialized_purpose_for_incarceration: Optional[
            str
        ] = specialized_purpose_for_incarceration
        self.custodial_authority = custodial_authority

    def __setattr__(self, name, value):
        restricted_setattr(self, "custodial_authority", name, value)


class StateSupervisionPeriod(IngestObject):
    """Class for information about a period of supervision. Referenced from IncarcerationSentence and
    SupervisionSentence.
    """

    def __init__(
        self,
        state_supervision_period_id=None,
        start_date=None,
        termination_date=None,
        state_code=None,
        county_code=None,
        supervision_site=None,
        admission_reason=None,
        termination_reason=None,
        supervision_level=None,
        conditions=None,
        supervising_officer=None,
        state_supervision_case_type_entries=None,
        supervision_type=None,
        custodial_authority=None,
    ):
        self.state_supervision_period_id: Optional[str] = state_supervision_period_id
        self.supervision_type: Optional[str] = supervision_type
        self.start_date: Optional[str] = start_date
        self.termination_date: Optional[str] = termination_date
        self.state_code: Optional[str] = state_code
        self.county_code: Optional[str] = county_code
        self.supervision_site: Optional[str] = supervision_site
        self.admission_reason: Optional[str] = admission_reason
        self.termination_reason: Optional[str] = termination_reason
        self.supervision_level: Optional[str] = supervision_level
        self.custodial_authority: Optional[str] = custodial_authority
        self.conditions: str = conditions

        self.supervising_officer: Optional[StateAgent] = supervising_officer
        self.state_supervision_case_type_entries: List[
            StateSupervisionCaseTypeEntry
        ] = (state_supervision_case_type_entries or [])

    def __setattr__(self, name, value):
        restricted_setattr(self, "state_supervision_case_type_entries", name, value)

    def create_state_agent(self, **kwargs) -> "StateAgent":
        self.supervising_officer = StateAgent(**kwargs)
        return self.supervising_officer

    def create_state_supervision_case_type_entry(
        self, **kwargs
    ) -> "StateSupervisionCaseTypeEntry":
        case_type = StateSupervisionCaseTypeEntry(**kwargs)
        self.state_supervision_case_type_entries.append(case_type)
        return case_type

    def get_state_supervision_case_type_entry_by_id(
        self, state_supervision_case_type_entry_id
    ) -> Optional["StateSupervisionCaseTypeEntry"]:
        return next(
            (
                cte
                for cte in self.state_supervision_case_type_entries
                if cte.state_supervision_case_type_entry_id
                == state_supervision_case_type_entry_id
            ),
            None,
        )

    def prune(self) -> "StateSupervisionPeriod":
        if not self.supervising_officer:
            self.supervising_officer = None

        self.state_supervision_case_type_entries = [
            c for c in self.state_supervision_case_type_entries if c
        ]

        return self

    def sort(self):
        self.state_supervision_case_type_entries.sort()


class StateSupervisionContact(IngestObject):
    """Class for information about an contact between a person and their supervising agent. Referenced from
    SupervisionPeriod.
    """

    def __init__(
        self,
        state_supervision_contact_id=None,
        contact_date=None,
        contact_reason=None,
        state_code=None,
        contact_type=None,
        contact_method=None,
        location=None,
        resulted_in_arrest=None,
        status=None,
        verified_employment=None,
        contacted_agent=None,
    ):
        self.state_supervision_contact_id: Optional[str] = state_supervision_contact_id
        self.contact_date: Optional[str] = contact_date
        self.contact_reason: Optional[str] = contact_reason
        self.state_code: Optional[str] = state_code
        self.contact_type: Optional[str] = contact_type
        self.contact_method: Optional[str] = contact_method
        self.location: Optional[str] = location
        self.resulted_in_arrest: Optional[str] = resulted_in_arrest
        self.status: Optional[str] = status
        self.verified_employment: Optional[str] = verified_employment

        self.contacted_agent: Optional[StateAgent] = contacted_agent

    def __setattr__(self, name, value):
        restricted_setattr(self, "contacted_agent", name, value)

    def create_state_agent(self, **kwargs) -> "StateAgent":
        self.contacted_agent = StateAgent(**kwargs)
        return self.contacted_agent

    def prune(self) -> "StateSupervisionContact":
        if not self.contacted_agent:
            self.contacted_agent = None
        return self


class StateSupervisionCaseTypeEntry(IngestObject):
    """Class for information about a supervision case type. Referenced from SupervisionPeriod."""

    def __init__(
        self, state_supervision_case_type_entry_id=None, case_type=None, state_code=None
    ):
        self.state_supervision_case_type_entry_id = state_supervision_case_type_entry_id
        self.case_type = case_type
        self.state_code = state_code

    def __setattr__(self, name, value):
        restricted_setattr(self, "state_code", name, value)


class StateIncarcerationIncident(IngestObject):
    """Class for information about an incident during incarceration. Referenced from IncarcerationPeriod."""

    def __init__(
        self,
        state_incarceration_incident_id=None,
        incident_type=None,
        incident_date=None,
        state_code=None,
        facility=None,
        location_within_facility=None,
        incident_details=None,
        responding_officer=None,
        state_incarceration_incident_outcomes=None,
    ):
        self.state_incarceration_incident_id: Optional[
            str
        ] = state_incarceration_incident_id
        self.incident_type: Optional[str] = incident_type
        self.incident_date: Optional[str] = incident_date
        self.state_code: Optional[str] = state_code
        self.facility: Optional[str] = facility
        self.location_within_facility: Optional[str] = location_within_facility
        self.incident_details: Optional[str] = incident_details

        self.responding_officer: Optional[StateAgent] = responding_officer
        self.state_incarceration_incident_outcomes: List[
            StateIncarcerationIncidentOutcome
        ] = (state_incarceration_incident_outcomes or [])

    def __setattr__(self, name, value):
        restricted_setattr(self, "incarceration_incident_outcomes", name, value)

    def create_state_agent(self, **kwargs) -> "StateAgent":
        self.responding_officer = StateAgent(**kwargs)
        return self.responding_officer

    def create_state_incarceration_incident_outcome(
        self, **kwargs
    ) -> "StateIncarcerationIncidentOutcome":
        outcome = StateIncarcerationIncidentOutcome(**kwargs)
        self.state_incarceration_incident_outcomes.append(outcome)
        return outcome

    def get_state_incarceration_incident_outcome_by_id(
        self, state_incarceration_incident_outcome_id
    ) -> Optional["StateIncarcerationIncidentOutcome"]:
        return next(
            (
                iio
                for iio in self.state_incarceration_incident_outcomes
                if iio.state_incarceration_incident_outcome_id
                == state_incarceration_incident_outcome_id
            ),
            None,
        )

    def prune(self) -> "StateIncarcerationIncident":
        if not self.responding_officer:
            self.responding_officer = None
        self.state_incarceration_incident_outcomes = [
            iio for iio in self.state_incarceration_incident_outcomes if iio
        ]
        return self

    def sort(self):
        self.state_incarceration_incident_outcomes.sort()


class StateIncarcerationIncidentOutcome(IngestObject):
    """Class for information about a incarceration incident outcome. Referenced from StateIncarcerationIncident."""

    def __init__(
        self,
        state_incarceration_incident_outcome_id=None,
        outcome_type=None,
        date_effective=None,
        hearing_date=None,
        report_date=None,
        state_code=None,
        outcome_description=None,
        punishment_length_days=None,
    ):
        self.state_incarceration_incident_outcome_id: Optional[
            str
        ] = state_incarceration_incident_outcome_id
        self.outcome_type: Optional[str] = outcome_type
        self.date_effective: Optional[str] = date_effective
        self.hearing_date: Optional[str] = hearing_date
        self.report_date: Optional[str] = report_date
        self.state_code: Optional[str] = state_code
        self.outcome_description: Optional[str] = outcome_description
        self.punishment_length_days: Optional[str] = punishment_length_days

    def __setattr__(self, name, value):
        restricted_setattr(self, "punishment_length_days", name, value)


class StateSupervisionViolationTypeEntry(IngestObject):
    """Class for type information about a violation during supervision."""

    def __init__(
        self,
        state_supervision_violation_type_entry_id=None,
        violation_type=None,
        state_code=None,
    ):
        self.state_supervision_violation_type_entry_id: Optional[
            str
        ] = state_supervision_violation_type_entry_id
        self.violation_type: Optional[str] = violation_type
        self.state_code: Optional[str] = state_code

    def __setattr__(self, name, value):
        restricted_setattr(self, "state_code", name, value)


class StateSupervisionViolatedConditionEntry(IngestObject):
    """Class for information about a condition on a person's supervision."""

    def __init__(
        self,
        state_supervision_violated_condition_entry_id=None,
        condition=None,
        state_code=None,
    ):
        self.state_supervision_violated_condition_entry_id: Optional[
            str
        ] = state_supervision_violated_condition_entry_id
        self.condition: Optional[str] = condition
        self.state_code: Optional[str] = state_code

    def __setattr__(self, name, value):
        restricted_setattr(self, "state_code", name, value)


class StateSupervisionViolation(IngestObject):
    """Class for information about a supervision violation. Referenced from SupervisionViolation."""

    def __init__(
        self,
        state_supervision_violation_id=None,
        violation_date=None,
        state_code=None,
        is_violent=None,
        is_sex_offense=None,
        state_supervision_violation_types=None,
        state_supervision_violated_conditions=None,
        state_supervision_violation_responses=None,
    ):
        self.state_supervision_violation_id: Optional[
            str
        ] = state_supervision_violation_id

        self.violation_date: Optional[str] = violation_date
        self.state_code: Optional[str] = state_code
        self.is_violent: Optional[str] = is_violent
        self.is_sex_offense: Optional[str] = is_sex_offense

        self.state_supervision_violation_types: List[
            StateSupervisionViolationTypeEntry
        ] = (state_supervision_violation_types or [])
        self.state_supervision_violated_conditions: List[
            StateSupervisionViolatedConditionEntry
        ] = (state_supervision_violated_conditions or [])
        self.state_supervision_violation_responses: List[
            StateSupervisionViolationResponse
        ] = (state_supervision_violation_responses or [])

    def __setattr__(self, name, value):
        restricted_setattr(self, "state_supervision_violation_responses", name, value)

    def create_state_supervision_violation_type_entry(
        self, **kwargs
    ) -> "StateSupervisionViolationTypeEntry":
        violation_type = StateSupervisionViolationTypeEntry(**kwargs)
        self.state_supervision_violation_types.append(violation_type)
        return violation_type

    def get_state_supervision_violation_type_entry_by_id(
        self, state_supervision_violation_type_entry_id
    ) -> Optional["StateSupervisionViolationTypeEntry"]:
        return next(
            (
                svt
                for svt in self.state_supervision_violation_types
                if svt.state_supervision_violation_type_entry_id
                == state_supervision_violation_type_entry_id
            ),
            None,
        )

    def create_state_supervision_violated_condition_entry(
        self, **kwargs
    ) -> "StateSupervisionViolatedConditionEntry":
        condition = StateSupervisionViolatedConditionEntry(**kwargs)
        self.state_supervision_violated_conditions.append(condition)
        return condition

    def get_state_supervision_violated_condition_entry_by_id(
        self, state_supervision_violated_condition_entry_id
    ) -> Optional["StateSupervisionViolatedConditionEntry"]:
        return next(
            (
                svc
                for svc in self.state_supervision_violated_conditions
                if svc.state_supervision_violated_condition_entry_id
                == state_supervision_violated_condition_entry_id
            ),
            None,
        )

    def create_state_supervision_violation_response(
        self, **kwargs
    ) -> "StateSupervisionViolationResponse":
        violation_response = StateSupervisionViolationResponse(**kwargs)
        self.state_supervision_violation_responses.append(violation_response)
        return violation_response

    def get_state_supervision_violation_response_by_id(
        self, state_supervision_violation_response_id
    ) -> Optional["StateSupervisionViolationResponse"]:
        return next(
            (
                vr
                for vr in self.state_supervision_violation_responses
                if vr.state_supervision_violation_response_id
                == state_supervision_violation_response_id
            ),
            None,
        )

    def prune(self) -> "StateSupervisionViolation":
        self.state_supervision_violated_conditions = [
            vc for vc in self.state_supervision_violated_conditions if vc
        ]
        self.state_supervision_violation_responses = [
            vr for vr in self.state_supervision_violation_responses if vr
        ]

        return self

    def sort(self):
        self.state_supervision_violation_responses.sort()


class StateSupervisionViolationResponseDecisionEntry(IngestObject):
    """Class for information about a condition on a person's supervision."""

    def __init__(
        self,
        state_supervision_violation_response_decision_entry_id=None,
        decision=None,
        state_code=None,
    ):
        self.state_supervision_violation_response_decision_entry_id: Optional[
            str
        ] = state_supervision_violation_response_decision_entry_id
        self.decision: Optional[str] = decision
        self.state_code: Optional[str] = state_code

    def __setattr__(self, name, value):
        restricted_setattr(self, "state_code", name, value)


class StateSupervisionViolationResponse(IngestObject):
    """Class for information about a supervision violation response. Referenced from
    StateSupervisionViolationResponse.
    """

    def __init__(
        self,
        state_supervision_violation_response_id=None,
        response_type=None,
        response_subtype=None,
        response_date=None,
        state_code=None,
        deciding_body_type=None,
        is_draft=None,
        supervision_violation_response_decisions=None,
        decision_agents=None,
    ):
        self.state_supervision_violation_response_id: Optional[
            str
        ] = state_supervision_violation_response_id
        self.response_type: Optional[str] = response_type
        self.response_subtype: Optional[str] = response_subtype
        self.response_date: Optional[str] = response_date
        self.state_code: Optional[str] = state_code
        self.deciding_body_type: Optional[str] = deciding_body_type
        self.is_draft: Optional[str] = is_draft
        self.state_supervision_violation_response_decisions: List[
            StateSupervisionViolationResponseDecisionEntry
        ] = (supervision_violation_response_decisions or [])
        self.decision_agents: List[StateAgent] = decision_agents or []

    def __setattr__(self, name, value):
        restricted_setattr(self, "decision_agents", name, value)

    def create_state_supervision_violation_response_decision_entry(
        self, **kwargs
    ) -> "StateSupervisionViolationResponseDecisionEntry":
        decision = StateSupervisionViolationResponseDecisionEntry(**kwargs)
        self.state_supervision_violation_response_decisions.append(decision)
        return decision

    def get_state_supervision_violation_response_decision_entry_by_id(
        self, state_supervision_violation_response_decision_entry_id
    ) -> Optional["StateSupervisionViolationResponseDecisionEntry"]:
        matching_svrdts = (
            svrdt
            for svrdt in self.state_supervision_violation_response_decisions
            if svrdt.state_supervision_violation_response_decision_entry_id
            == state_supervision_violation_response_decision_entry_id
        )

        return next(matching_svrdts, None)

    def create_state_agent(self, **kwargs) -> "StateAgent":
        decision_agent = StateAgent(**kwargs)
        self.decision_agents.append(decision_agent)
        return decision_agent

    def prune(self) -> "StateSupervisionViolationResponse":
        self.state_supervision_violation_response_decisions = [
            d for d in self.state_supervision_violation_response_decisions if d
        ]
        self.decision_agents = [da for da in self.decision_agents if da]
        return self

    def sort(self):
        self.decision_agents.sort()


class StateAgent(IngestObject):
    """Class for information about some agent working within the justice system. Referenced from several state-level
    entities.
    """

    def __init__(
        self,
        state_agent_id=None,
        agent_type=None,
        state_code=None,
        full_name=None,
        surname=None,
        given_names=None,
        middle_names=None,
        name_suffix=None,
    ):
        self.state_agent_id: Optional[str] = state_agent_id
        self.agent_type: Optional[str] = agent_type
        self.state_code: Optional[str] = state_code
        self.full_name: Optional[str] = full_name
        self.surname: Optional[str] = surname
        self.given_names: Optional[str] = given_names
        self.middle_names: Optional[str] = middle_names
        self.name_suffix: Optional[str] = name_suffix

    def __setattr__(self, name, value):
        restricted_setattr(self, "name_suffix", name, value)


class StateProgramAssignment(IngestObject):
    """Class for information about a program assignment. Referenced from StatePerson, StateIncarcerationPeriod, and
    StateSupervisionPeriod.
    """

    def __init__(
        self,
        state_program_assignment_id=None,
        participation_status=None,
        referral_date=None,
        start_date=None,
        discharge_date=None,
        state_code=None,
        program_id=None,
        program_location_id=None,
        referral_metadata=None,
        referring_agent=None,
    ):
        self.state_program_assignment_id: Optional[str] = state_program_assignment_id
        self.participation_status: Optional[str] = participation_status
        self.referral_date: Optional[str] = referral_date
        self.start_date: Optional[str] = start_date
        self.discharge_date: Optional[str] = discharge_date
        self.state_code: Optional[str] = state_code
        self.program_id: Optional[str] = program_id
        self.program_location_id: Optional[str] = program_location_id
        self.referral_metadata: Optional[str] = referral_metadata

        self.referring_agent: Optional[StateAgent] = referring_agent

    def __setattr__(self, name, value):
        restricted_setattr(self, "referring_agent", name, value)

    def create_state_agent(self, **kwargs) -> "StateAgent":
        self.referring_agent = StateAgent(**kwargs)
        return self.referring_agent

    def prune(self) -> "StateProgramAssignment":
        if not self.referring_agent:
            self.referring_agent = None
        return self


class StateEarlyDischarge(IngestObject):
    """Class for information about an Early Discharge. Referenced from StateIncarcerationSentence and
    StateSupervisionSentence.
    """

    def __init__(
        self,
        state_early_discharge_id=None,
        request_date=None,
        decision_date=None,
        decision=None,
        decision_status=None,
        deciding_body_type=None,
        requesting_body_type=None,
        state_code=None,
        county_code=None,
    ):
        self.state_early_discharge_id: Optional[str] = state_early_discharge_id
        self.request_date: Optional[str] = request_date
        self.decision_date: Optional[str] = decision_date
        self.decision: Optional[str] = decision
        self.decision_status: Optional[str] = decision_status
        self.deciding_body_type: Optional[str] = deciding_body_type
        self.requesting_body_type: Optional[str] = requesting_body_type
        self.state_code: Optional[str] = state_code
        self.county_code: Optional[str] = county_code

    def __setattr__(self, name, value):
        restricted_setattr(self, "county_code", name, value)


def eq(self, other, exclude=None):
    if other is None:
        return False
    if exclude is None:
        return self.__dict__ == other.__dict__

    return _without_exclusions(self, exclude) == _without_exclusions(other, exclude)


def _without_exclusions(obj, exclude=None):
    if exclude is None:
        exclude = []
    return {k: v for k, v in obj.__dict__.items() if k not in exclude}


def to_bool(obj, exclude=None):
    if exclude is None:
        exclude = []
    return any(
        any(v) if isinstance(v, list) else v
        for k, v in obj.__dict__.items()
        if k not in exclude
    )


def to_string(obj, exclude=None):
    if exclude is None:
        exclude = []
    out = [obj.__class__.__name__ + ":"]
    for key, val in vars(obj).items():
        if key in exclude:
            continue
        if isinstance(val, list):
            for index, elem in enumerate(val):
                out += f"{key}[{index}]: {elem}".split("\n")
        elif val is not None:
            out += f"{key}: {val}".split("\n")
    return "\n   ".join(out)


def to_repr(obj, exclude=None):
    if exclude is None:
        exclude = []
    args = []
    for key, val in vars(obj).items():
        if key in exclude:
            continue
        if val is not None:
            args.append(f"{key}={repr(val)}")

    return f"{obj.__class__.__name__}({', '.join(args)})"


def restricted_setattr(self, last_field, name, value):
    if isinstance(value, str) and (value == "" or value.isspace()):
        value = None
    if hasattr(self, last_field) and not hasattr(self, name):
        raise AttributeError(f"No field {name} in object {type(self)}")
    self.__dict__[name] = value
