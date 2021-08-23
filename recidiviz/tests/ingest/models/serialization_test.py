# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests for ingest/models/serialization.py"""
import copy
import unittest

from mock import Mock, patch

from recidiviz.common import common_utils
from recidiviz.ingest.models import ingest_info, ingest_info_pb2, serialization


class TestSerialization(unittest.TestCase):
    """Tests for serialization"""

    def _create_generated_id(self) -> str:
        self.counter += 1
        return str(self.counter) + common_utils.GENERATED_ID_SUFFIX

    def setUp(self) -> None:
        self.counter = 0

    @patch("recidiviz.common.common_utils.create_generated_id")
    def test_convert_ingest_info_id_is_generated(self, mock_create: Mock) -> None:
        mock_create.side_effect = self._create_generated_id
        info = ingest_info.IngestInfo()
        person = info.create_person()
        person.surname = "testname"
        person.create_booking()

        expected_proto = ingest_info_pb2.IngestInfo()
        proto_person = expected_proto.people.add()
        proto_person.surname = "testname"
        proto_person.person_id = "1_GENERATE"
        proto_booking = expected_proto.bookings.add()
        proto_booking.booking_id = "2_GENERATE"
        proto_person.booking_ids.append(proto_booking.booking_id)

        proto = serialization.convert_ingest_info_to_proto(info)
        assert proto == expected_proto

        info_back = serialization.convert_proto_to_ingest_info(proto)
        assert info_back == info

    def test_convert_ingest_info_id_is_not_generated(self) -> None:
        info = ingest_info.IngestInfo()
        person = info.create_person()
        person.person_id = "id1"
        person.surname = "testname"
        booking = person.create_booking()
        booking.booking_id = "id2"
        booking.admission_date = "testdate"

        expected_proto = ingest_info_pb2.IngestInfo()
        proto_person = expected_proto.people.add()
        proto_person.person_id = "id1"
        proto_person.surname = "testname"
        proto_person.booking_ids.append("id2")
        proto_booking = expected_proto.bookings.add()
        proto_booking.booking_id = "id2"
        proto_booking.admission_date = "testdate"

        proto = serialization.convert_ingest_info_to_proto(info)
        assert expected_proto == proto

        info_back = serialization.convert_proto_to_ingest_info(proto)
        assert info_back == info

    @patch("recidiviz.common.common_utils.create_generated_id")
    def test_convert_ingest_info_one_charge_to_one_bond(
        self, mock_create: Mock
    ) -> None:
        mock_create.side_effect = self._create_generated_id
        info = ingest_info.IngestInfo()
        person = info.create_person()
        person.person_id = "id1"

        booking = person.create_booking()
        booking.booking_id = "id1"
        charge = booking.create_charge()
        charge.charge_id = "id1"
        bond1 = charge.create_bond()
        bond1.amount = "$1"
        charge = booking.create_charge()
        charge.charge_id = "id2"
        bond2 = charge.create_bond()
        bond2.amount = "$1"

        expected_proto = ingest_info_pb2.IngestInfo()
        proto_person = expected_proto.people.add()
        proto_person.person_id = "id1"
        proto_person.booking_ids.append("id1")
        proto_booking = expected_proto.bookings.add()
        proto_booking.booking_id = "id1"
        proto_booking.charge_ids.extend(["id1", "id2"])
        proto_charge = expected_proto.charges.add()
        proto_charge.charge_id = "id1"
        proto_bond1 = expected_proto.bonds.add()
        proto_bond1.amount = "$1"
        proto_bond1.bond_id = "1_GENERATE"
        proto_charge.bond_id = proto_bond1.bond_id
        proto_charge = expected_proto.charges.add()
        proto_charge.charge_id = "id2"
        proto_bond2 = expected_proto.bonds.add()
        proto_bond2.amount = "$1"
        proto_bond2.bond_id = "2_GENERATE"
        proto_charge.bond_id = proto_bond2.bond_id

        proto = serialization.convert_ingest_info_to_proto(info)
        assert expected_proto == proto

        info_back = serialization.convert_proto_to_ingest_info(proto)
        assert info_back == info

    @patch("recidiviz.common.common_utils.create_generated_id")
    def test_convert_ingest_info_many_charge_to_one_bond(
        self, mock_create: Mock
    ) -> None:
        mock_create.side_effect = self._create_generated_id
        info = ingest_info.IngestInfo()
        person = info.create_person()
        person.person_id = "id1"

        booking = person.create_booking()
        booking.booking_id = "id1"
        charge = booking.create_charge()
        charge.charge_id = "id1"
        bond1 = charge.create_bond()
        bond1.amount = "$1"
        charge = booking.create_charge()
        charge.charge_id = "id2"
        charge.bond = bond1

        expected_proto = ingest_info_pb2.IngestInfo()
        proto_person = expected_proto.people.add()
        proto_person.person_id = "id1"
        proto_person.booking_ids.append("id1")
        proto_booking = expected_proto.bookings.add()
        proto_booking.booking_id = "id1"
        proto_booking.charge_ids.extend(["id1", "id2"])
        proto_charge = expected_proto.charges.add()
        proto_charge.charge_id = "id1"
        proto_bond = expected_proto.bonds.add()
        proto_bond.amount = "$1"
        proto_bond.bond_id = "1_GENERATE"
        proto_charge.bond_id = proto_bond.bond_id
        proto_charge = expected_proto.charges.add()
        proto_charge.charge_id = "id2"
        proto_charge.bond_id = proto_bond.bond_id

        proto = serialization.convert_ingest_info_to_proto(info)
        assert len(proto.bonds) == 1
        assert expected_proto == proto

        info_back = serialization.convert_proto_to_ingest_info(proto)
        assert info_back == info

    def test_serializable(self) -> None:
        info = ingest_info.IngestInfo()
        person = info.create_person()
        person.person_id = "id1"

        booking = person.create_booking()
        booking.booking_id = "id1"
        charge = booking.create_charge()
        charge.charge_id = "id1"
        bond1 = charge.create_bond()
        bond1.amount = "$1"
        charge = booking.create_charge()
        charge.charge_id = "id2"
        bond2 = charge.create_bond()
        bond2.amount = "$1"

        converted_info = serialization.ingest_info_from_serializable(
            serialization.ingest_info_to_serializable(info)
        )

        assert converted_info == info

    def test_convert_ingest_info_state_entities(self) -> None:
        # Arrange Python ingest info
        info = ingest_info.IngestInfo()
        person = info.create_state_person()
        person.state_person_id = "person1"
        person.surname = "testname"

        race = person.create_state_person_race()
        race.state_person_race_id = "race1"
        race.race = "white"
        ethnicity = person.create_state_person_ethnicity()
        ethnicity.state_person_ethnicity_id = "ethnicity1"
        ethnicity.ethnicity = "non-hispanic"
        external_id = person.create_state_person_external_id()
        external_id.state_person_external_id_id = "external_id1"
        external_id.id_type = "contrived"
        alias = person.create_state_alias()
        alias.state_alias_id = "alias1"
        alias.surname = "testerson"
        assessment = person.create_state_assessment()
        assessment.state_assessment_id = "assessment1"
        assessment.assessment_score = "42"
        supervising_officer = person.create_state_agent()
        supervising_officer.state_agent_id = "supervising_officer1"
        supervising_officer.full_name = "Officer Supervising"

        assessment_agent = assessment.create_state_agent()
        assessment_agent.state_agent_id = "agent1"
        assessment_agent.full_name = "Officer Jones"

        program_assignment = person.create_state_program_assignment()
        program_assignment.state_program_assignment_id = "assignment1"
        program_assignment.program_id = "program_id1"

        program_assignment_agent = program_assignment.create_state_agent()
        program_assignment_agent.state_agent_id = "program_agent1"
        program_assignment_agent.full_name = "Officer Program"

        group = person.create_state_sentence_group()
        group.state_sentence_group_id = "group1"

        fine = group.create_state_fine()
        fine.state_fine_id = "fine1"

        incarceration_sentence = group.create_state_incarceration_sentence()
        incarceration_sentence.state_incarceration_sentence_id = "is1"
        early_discharge1 = incarceration_sentence.create_state_early_discharge()
        early_discharge1.state_early_discharge_id = "early_discharge1"
        charge1 = incarceration_sentence.create_state_charge()
        charge1.state_charge_id = "charge1"
        charge1.classification_type = "F"
        incarceration_period = (
            incarceration_sentence.create_state_incarceration_period()
        )
        incarceration_period.state_incarceration_period_id = "ip1"
        incarceration_period.status = "IN_CUSTODY"
        incarceration_period.specialized_purpose_for_incarceration = (
            "SHOCK INCARCERATION"
        )
        incarceration_period.state_program_assignments = [program_assignment]
        incident = incarceration_period.create_state_incarceration_incident()
        incident.state_incarceration_incident_id = "incident1"
        incident.incident_type = "FISTICUFFS"
        incident_outcome = incident.create_state_incarceration_incident_outcome()
        incident_outcome.state_incarceration_incident_outcome_id = "incident1-1"
        incident_outcome.outcome_type = "FINE"

        incident_agent = incident.create_state_agent()
        incident_agent.state_agent_id = "agent2"
        incident_agent.full_name = "Officer Thompson"

        decision = incarceration_period.create_state_parole_decision()
        decision.state_parole_decision_id = "decision1"

        decision_agent = decision.create_state_agent()
        decision_agent.state_agent_id = "agent3"
        decision_agent.full_name = "Officer Barkley"

        supervision_sentence = group.create_state_supervision_sentence()
        supervision_sentence.state_supervision_sentence_id = "ss1"
        early_discharge2 = supervision_sentence.create_state_early_discharge()
        early_discharge2.state_early_discharge_id = "early_discharge2"
        charge2 = supervision_sentence.create_state_charge()
        charge2.state_charge_id = "charge2"
        charge2.classification_type = "M"
        supervision_period = supervision_sentence.create_state_supervision_period()
        supervision_period.state_supervision_period_id = "sp1"
        supervision_period.status = "TERMINATED"
        supervision_period_agent = supervision_period.create_state_agent()
        supervision_period_agent.state_agent_id = "agentPO"
        supervision_period_agent.full_name = "Officer Paroley"
        supervision_period.state_program_assignments = [program_assignment]

        supervision_case_type_entry = (
            supervision_period.create_state_supervision_case_type_entry()
        )
        supervision_case_type_entry.case_type = "case_type"
        supervision_case_type_entry.state_supervision_case_type_entry_id = (
            "case_type_entry_id"
        )

        supervision_contact = supervision_period.create_state_supervision_contact()
        supervision_contact.state_supervision_contact_id = "supervision_contact_id"
        supervision_contact.contact_type = "contact_type"
        supervision_contact.contact_method = "contact_method"
        supervision_contacted_agent = supervision_contact.create_state_agent()
        supervision_contacted_agent.state_agent_id = "agentPO"
        supervision_contacted_agent.full_name = "Officer Paroley"

        violation = supervision_period.create_state_supervision_violation()
        violation.state_supervision_violation_id = "violation1"
        violation.violated_conditions = "cond"
        violation.is_violent = "false"

        violation_type = violation.create_state_supervision_violation_type_entry()
        violation_type.state_supervision_violation_type_entry_id = "violation_type_id"
        violation_type.violation_type = "FELONY"

        violated_condition = (
            violation.create_state_supervision_violated_condition_entry()
        )
        violated_condition.state_supervision_violated_condition_entry_id = (
            "condition_id"
        )
        violated_condition.condition = "CURFEW"

        response = violation.create_state_supervision_violation_response()
        response.state_supervision_violation_response_id = "response1"
        response_decision_agent = response.create_state_agent()
        response_decision_agent.state_agent_id = "agentTERM"
        response_decision_agent.full_name = "Officer Termy"

        response_decision = (
            response.create_state_supervision_violation_response_decision_entry()
        )
        response_decision.state_supervision_violation_response_decision_entry_id = (
            "response_decision_id"
        )
        response_decision.decision = "REVOCATION"
        response_decision.revocation_type = "REINCARCERATION"

        bond = charge1.create_state_bond()
        bond.state_bond_id = "bond1"

        court_case = charge2.create_state_court_case()
        court_case.state_court_case_id = "case1"

        court_case_agent = court_case.create_state_agent()
        court_case_agent.state_agent_id = "agentJ"
        court_case_agent.full_name = "Judge Agent"

        # Arrange Proto ingest info
        expected_proto = ingest_info_pb2.IngestInfo()
        person_pb = expected_proto.state_people.add()
        person_pb.state_person_id = "person1"
        person_pb.surname = "testname"

        person_pb.state_person_race_ids.append("race1")
        race_pb = expected_proto.state_person_races.add()
        race_pb.state_person_race_id = "race1"
        race_pb.race = "white"
        person_pb.state_person_ethnicity_ids.append("ethnicity1")
        ethnicity_pb = expected_proto.state_person_ethnicities.add()
        ethnicity_pb.state_person_ethnicity_id = "ethnicity1"
        ethnicity_pb.ethnicity = "non-hispanic"
        person_pb.state_person_external_ids_ids.append("contrived:external_id1")
        external_id_pb = expected_proto.state_person_external_ids.add()
        external_id_pb.state_person_external_id_id = "contrived:external_id1"
        external_id_pb.id_type = "contrived"
        person_pb.state_alias_ids.append("alias1")
        alias_pb = expected_proto.state_aliases.add()
        alias_pb.state_alias_id = "alias1"
        alias_pb.surname = "testerson"
        person_pb.state_assessment_ids.append("assessment1")
        assessment_pb = expected_proto.state_assessments.add()
        assessment_pb.state_assessment_id = "assessment1"
        assessment_pb.assessment_score = "42"
        person_pb.supervising_officer_id = "supervising_officer1"
        supervising_officer_pb = expected_proto.state_agents.add()
        supervising_officer_pb.state_agent_id = "supervising_officer1"
        supervising_officer_pb.full_name = "Officer Supervising"

        assessment_pb.conducting_agent_id = "agent1"
        assessment_agent_pb = expected_proto.state_agents.add()
        assessment_agent_pb.state_agent_id = "agent1"
        assessment_agent_pb.full_name = "Officer Jones"

        person_pb.state_program_assignment_ids.append("assignment1")
        program_assignment_pb = expected_proto.state_program_assignments.add()
        program_assignment_pb.state_program_assignment_id = "assignment1"
        program_assignment_pb.program_id = "program_id1"
        program_assignment_pb.referring_agent_id = "program_agent1"
        program_assignment_agent_pb = expected_proto.state_agents.add()
        program_assignment_agent_pb.state_agent_id = "program_agent1"
        program_assignment_agent_pb.full_name = "Officer Program"

        person_pb.state_sentence_group_ids.append("group1")
        group_pb = expected_proto.state_sentence_groups.add()
        group_pb.state_sentence_group_id = "group1"

        group_pb.state_fine_ids.append("fine1")
        fine_pb = expected_proto.state_fines.add()
        fine_pb.state_fine_id = "fine1"

        group_pb.state_supervision_sentence_ids.append("ss1")
        supervision_sentence_pb = expected_proto.state_supervision_sentences.add()
        supervision_sentence_pb.state_supervision_sentence_id = "ss1"
        supervision_sentence_pb.state_early_discharge_ids.append("early_discharge2")
        early_discharge2_pb = expected_proto.state_early_discharges.add()
        early_discharge2_pb.state_early_discharge_id = "early_discharge2"
        supervision_sentence_pb.state_charge_ids.append("charge2")
        charge2_pb = expected_proto.state_charges.add()
        charge2_pb.state_charge_id = "charge2"
        charge2_pb.classification_type = "M"
        supervision_sentence_pb.state_supervision_period_ids.append("sp1")
        supervision_period_pb = expected_proto.state_supervision_periods.add()
        supervision_period_pb.state_supervision_period_id = "sp1"
        supervision_period_pb.status = "TERMINATED"
        supervision_period_pb.state_program_assignment_ids.append("assignment1")

        # An ordering requirement in the proto equality check at the end of this
        # test requires that this agent be added after agent1 and before agentPO
        court_case_agent_pb = expected_proto.state_agents.add()
        court_case_agent_pb.state_agent_id = "agentJ"
        court_case_agent_pb.full_name = "Judge Agent"

        supervision_period_pb.supervising_officer_id = "agentPO"
        supervision_period_agent_pb = expected_proto.state_agents.add()
        supervision_period_agent_pb.state_agent_id = "agentPO"
        supervision_period_agent_pb.full_name = "Officer Paroley"

        supervision_case_type_entry_pb = (
            expected_proto.state_supervision_case_type_entries.add()
        )
        supervision_case_type_entry_pb.state_supervision_case_type_entry_id = (
            "case_type_entry_id"
        )
        supervision_case_type_entry_pb.case_type = "case_type"
        supervision_period_pb.state_supervision_case_type_entry_ids.append(
            "case_type_entry_id"
        )

        supervision_contact_pb = expected_proto.state_supervision_contacts.add()
        supervision_contact_pb.state_supervision_contact_id = "supervision_contact_id"
        supervision_contact_pb.contact_type = "contact_type"
        supervision_contact_pb.contact_method = "contact_method"
        supervision_contact_pb.contacted_agent_id = "agentPO"
        supervision_period_pb.state_supervision_contact_ids.append(
            "supervision_contact_id"
        )

        supervision_period_pb.state_supervision_violation_entry_ids.append("violation1")
        violation_pb = expected_proto.state_supervision_violations.add()
        violation_pb.state_supervision_violation_id = "violation1"
        violation_pb.is_violent = "false"
        violation_pb.violated_conditions = "cond"
        violation_pb.state_supervision_violation_type_entry_ids.append(
            "violation_type_id"
        )
        violation_type_pb = (
            expected_proto.state_supervision_violation_type_entries.add()
        )
        violation_type_pb.state_supervision_violation_type_entry_id = (
            "violation_type_id"
        )
        violation_type_pb.violation_type = "FELONY"

        violation_pb.state_supervision_violated_condition_entry_ids.append(
            "condition_id"
        )
        proto_violation_type_pb = (
            expected_proto.state_supervision_violated_condition_entries.add()
        )
        proto_violation_type_pb.state_supervision_violated_condition_entry_id = (
            "condition_id"
        )
        proto_violation_type_pb.condition = "CURFEW"

        violation_pb.state_supervision_violation_response_ids.append("response1")
        response_pb = expected_proto.state_supervision_violation_responses.add()
        response_pb.state_supervision_violation_response_id = "response1"
        response_pb.decision_agent_ids.append("agentTERM")
        response_decision_agent_pb = expected_proto.state_agents.add()
        response_decision_agent_pb.state_agent_id = "agentTERM"
        response_decision_agent_pb.full_name = "Officer Termy"
        response_decision_pb = (
            expected_proto.state_supervision_violation_response_decision_entries.add()
        )
        response_decision_pb.state_supervision_violation_response_decision_entry_id = (
            "response_decision_id"
        )
        response_decision_pb.decision = "REVOCATION"
        response_decision_pb.revocation_type = "REINCARCERATION"
        response_pb.state_supervision_violation_response_decision_entry_ids.append(
            "response_decision_id"
        )

        group_pb.state_incarceration_sentence_ids.append("is1")
        incarceration_sentence_pb = expected_proto.state_incarceration_sentences.add()
        incarceration_sentence_pb.state_incarceration_sentence_id = "is1"
        incarceration_sentence_pb.state_early_discharge_ids.append("early_discharge1")
        early_discharge1_pb = expected_proto.state_early_discharges.add()
        early_discharge1_pb.state_early_discharge_id = "early_discharge1"
        incarceration_sentence_pb.state_charge_ids.append("charge1")
        charge1_pb = expected_proto.state_charges.add()
        charge1_pb.state_charge_id = "charge1"
        charge1_pb.classification_type = "F"
        incarceration_sentence_pb.state_incarceration_period_ids.append("ip1")
        incarceration_period_pb = expected_proto.state_incarceration_periods.add()
        incarceration_period_pb.state_incarceration_period_id = "ip1"
        incarceration_period_pb.status = "IN_CUSTODY"
        incarceration_period_pb.specialized_purpose_for_incarceration = (
            "SHOCK INCARCERATION"
        )
        incarceration_period_pb.state_incarceration_incident_ids.append("incident1")
        incident_pb = expected_proto.state_incarceration_incidents.add()
        incident_pb.state_incarceration_incident_id = "incident1"
        incident_pb.incident_type = "FISTICUFFS"
        incarceration_period_pb.state_program_assignment_ids.append("assignment1")

        incident_pb.responding_officer_id = "agent2"
        incident_agent_pb = expected_proto.state_agents.add()
        incident_agent_pb.state_agent_id = "agent2"
        incident_agent_pb.full_name = "Officer Thompson"

        incident_pb.state_incarceration_incident_outcome_ids.append("incident1-1")
        incident_outcome_pb = expected_proto.state_incarceration_incident_outcomes.add()
        incident_outcome_pb.state_incarceration_incident_outcome_id = "incident1-1"
        incident_outcome_pb.outcome_type = "FINE"

        incarceration_period_pb.state_parole_decision_ids.append("decision1")
        decision_pb = expected_proto.state_parole_decisions.add()
        decision_pb.state_parole_decision_id = "decision1"

        decision_pb.decision_agent_ids.append("agent3")
        decision_agent_pb = expected_proto.state_agents.add()
        decision_agent_pb.state_agent_id = "agent3"
        decision_agent_pb.full_name = "Officer Barkley"

        charge1_pb.state_bond_id = "bond1"
        bond_pb = expected_proto.state_bonds.add()
        bond_pb.state_bond_id = "bond1"

        charge2_pb.state_court_case_id = "case1"
        court_case_pb = expected_proto.state_court_cases.add()
        court_case_pb.state_court_case_id = "case1"

        court_case_pb.judge_id = "agentJ"

        expected_info = copy.deepcopy(info)
        # Act & Assert

        proto = serialization.convert_ingest_info_to_proto(info)
        assert expected_proto == proto

        info_back = serialization.convert_proto_to_ingest_info(proto)
        assert info_back == expected_info

        # Assert that none of the proto's collections are empty, i.e. we've
        # tested all of the object graph
        proto_classes = [field.name for field in proto.DESCRIPTOR.fields]
        for cls in proto_classes:
            if cls.startswith("state_"):
                assert proto.__getattribute__(cls)
