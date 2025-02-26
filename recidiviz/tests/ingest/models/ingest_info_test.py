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

"""Tests for ingest_info"""

import unittest
from typing import List, Optional, Type

from google.protobuf.message import Message

from recidiviz.ingest.models import ingest_info
from recidiviz.ingest.models.ingest_info import IngestInfo, IngestObject
from recidiviz.ingest.models.ingest_info_pb2 import (
    StateAssessment,
    StateCharge,
    StateCourtCase,
    StateEarlyDischarge,
    StateIncarcerationIncident,
    StateIncarcerationIncidentOutcome,
    StateIncarcerationPeriod,
    StateIncarcerationSentence,
    StatePerson,
    StateProgramAssignment,
    StateSupervisionContact,
    StateSupervisionPeriod,
    StateSupervisionSentence,
    StateSupervisionViolation,
    StateSupervisionViolationResponse,
)


class FieldsDontMatchError(Exception):
    pass


class TestIngestInfo(unittest.TestCase):
    """Tests for ingest_info"""

    def test_proto_fields_match(self) -> None:
        def _verify_fields(
            proto: Type[Message],
            ingest_info_source: IngestObject,
            ignore: Optional[List[str]] = None,
        ) -> None:
            ignore = ignore or []
            proto_fields = [field.name for field in proto.DESCRIPTOR.fields]
            source_fields = vars(ingest_info_source)
            for field in proto_fields:
                if field not in source_fields and field not in ignore:
                    raise FieldsDontMatchError(
                        f"Field '{field}' exists in '{proto.__name__}' proto"
                        " but not in the IngestInfo object"
                    )

            for field in source_fields:
                if field not in proto_fields and field not in ignore:
                    raise FieldsDontMatchError(
                        f"Field '{field}' exists in '{proto.__name__}'"
                        " IngestInfo object but not in the proto object"
                    )

        # These should only contain fields that are listed in ingest_info.proto
        # as ids but listed in ingest_info.py as full objects. Think carefully
        # before adding anything else.
        state_person_fields_ignore = [
            "state_person_race_ids",
            "state_person_races",
            "state_person_ethnicity_ids",
            "state_person_ethnicities",
            "state_alias_ids",
            "state_aliases",
            "state_person_external_ids_ids",
            "state_person_external_ids",
            "state_assessment_ids",
            "state_assessments",
            "state_program_assignment_ids",
            "state_program_assignments",
            "state_incarceration_incident_ids",
            "state_incarceration_incidents",
            "state_supervision_violation_ids",
            "state_supervision_violations",
            "state_supervision_contact_ids",
            "state_supervision_contacts",
            "supervising_officer_id",
            "supervising_officer",
            "state_supervision_sentence_ids",
            "state_supervision_sentences",
            "state_incarceration_sentence_ids",
            "state_incarceration_sentences",
            "state_incarceration_period_ids",
            "state_incarceration_periods",
            "state_supervision_period_ids",
            "state_supervision_periods",
        ]
        assessment_fields_ignore = ["conducting_agent_id", "conducting_agent"]
        supervision_sentence_fields_ignore = [
            "state_charge_ids",
            "state_charges",
            "state_early_discharge_ids",
            "state_early_discharges",
        ]
        incarceration_sentence_fields_ignore = [
            "state_charge_ids",
            "state_charges",
            "state_early_discharge_ids",
            "state_early_discharges",
        ]
        state_charge_fields_ignore = [
            "state_court_case_id",
            "state_court_case",
        ]
        state_court_case_fields_ignore = ["judge_id", "judge"]
        incarceration_incident_fields_ignore = [
            "responding_officer_id",
            "responding_officer",
            "state_incarceration_incident_outcomes",
            "state_incarceration_incident_outcome_ids",
        ]
        supervision_period_fields_ignore = [
            "supervising_officer_id",
            "supervising_officer",
            "state_supervision_case_type_entry_ids",
            "state_supervision_case_type_entries",
        ]
        supervision_contacts_fields_ignore = ["contacted_agent_id", "contacted_agent"]
        supervision_violation_fields_ignore = [
            "state_supervision_violation_response_ids",
            "state_supervision_violation_responses",
            "state_supervision_violated_condition_entry_ids",
            "state_supervision_violated_conditions",
            "state_supervision_violation_type_entry_ids",
            "state_supervision_violation_types",
        ]
        supervision_violation_response_fields_ignore = [
            "decision_agent_ids",
            "decision_agents",
            "state_supervision_violation_response_decision_entry_ids",
            "state_supervision_violation_response_decisions",
        ]
        program_assignment_fields_ignore = ["referring_agent", "referring_agent_id"]

        _verify_fields(
            StatePerson, ingest_info.StatePerson(), state_person_fields_ignore
        )
        _verify_fields(
            StateAssessment, ingest_info.StateAssessment(), assessment_fields_ignore
        )
        _verify_fields(
            StateSupervisionSentence,
            ingest_info.StateSupervisionSentence(),
            supervision_sentence_fields_ignore,
        )
        _verify_fields(
            StateIncarcerationSentence,
            ingest_info.StateIncarcerationSentence(),
            incarceration_sentence_fields_ignore,
        )
        _verify_fields(StateEarlyDischarge, ingest_info.StateEarlyDischarge())
        _verify_fields(
            StateCharge, ingest_info.StateCharge(), state_charge_fields_ignore
        )
        _verify_fields(
            StateCourtCase, ingest_info.StateCourtCase(), state_court_case_fields_ignore
        )
        _verify_fields(
            StateIncarcerationPeriod,
            ingest_info.StateIncarcerationPeriod(),
        )
        _verify_fields(
            StateSupervisionPeriod,
            ingest_info.StateSupervisionPeriod(),
            supervision_period_fields_ignore,
        )
        _verify_fields(
            StateSupervisionContact,
            ingest_info.StateSupervisionContact(),
            supervision_contacts_fields_ignore,
        )
        _verify_fields(
            StateIncarcerationIncident,
            ingest_info.StateIncarcerationIncident(),
            incarceration_incident_fields_ignore,
        )
        _verify_fields(
            StateIncarcerationIncidentOutcome,
            ingest_info.StateIncarcerationIncidentOutcome(),
            incarceration_incident_fields_ignore,
        )
        _verify_fields(
            StateSupervisionViolation,
            ingest_info.StateSupervisionViolation(),
            supervision_violation_fields_ignore,
        )
        _verify_fields(
            StateSupervisionViolationResponse,
            ingest_info.StateSupervisionViolationResponse(),
            supervision_violation_response_fields_ignore,
        )
        _verify_fields(
            StateProgramAssignment,
            ingest_info.StateProgramAssignment(),
            program_assignment_fields_ignore,
        )

    def test_bool_falsy(self) -> None:
        ii = IngestInfo()
        person = ii.create_state_person()
        person.create_state_incarceration_sentence().create_state_charge()
        person.create_state_incarceration_sentence()
        self.assertFalse(ii)

    def test_bool_truthy(self) -> None:
        ii = IngestInfo()
        person = ii.create_state_person()
        person.create_state_incarceration_sentence().create_state_charge(
            offense_date="1/2/3"
        )
        person.create_state_incarceration_sentence()
        self.assertTrue(ii)

    def test_sort(self) -> None:
        b1 = ingest_info.StateSupervisionSentence(date_imposed="1")
        b2 = ingest_info.StateSupervisionSentence(date_imposed="2")

        ii = IngestInfo(
            state_people=[ingest_info.StatePerson(state_supervision_sentences=[b1, b2])]
        )
        ii_reversed = IngestInfo(
            state_people=[ingest_info.StatePerson(state_supervision_sentences=[b2, b1])]
        )

        self.assertNotEqual(ii, ii_reversed)
        ii.sort()
        ii_reversed.sort()
        self.assertEqual(ii, ii_reversed)
