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

"""Tests for ingest/ingest_utils.py."""

from mock import Mock, PropertyMock, patch

from recidiviz.common import common_utils
from recidiviz.ingest.models import ingest_info, ingest_info_pb2
from recidiviz.ingest.scrape import constants, ingest_utils


def fake_modules(*names):
    modules = []
    for name in names:
        fake_module = Mock()
        type(fake_module).name = PropertyMock(return_value=name)
        modules.append(fake_module)
    return modules


class TestIngestUtils:
    """Tests for regions.py."""

    def _create_generated_id(self):
        self.counter += 1
        return str(self.counter) + common_utils.GENERATED_ID_SUFFIX

    def setup_method(self, _):
        self.counter = 0

    @patch('pkgutil.iter_modules',
           return_value=fake_modules('us_ny', 'us_pa', 'us_vt', 'us_pa_greene'))
    def test_validate_regions_one_ok(self, _mock_modules):
        assert ingest_utils.validate_regions(['us_ny']) == {'us_ny'}

    @patch('pkgutil.iter_modules',
           return_value=fake_modules('us_ny', 'us_pa', 'us_vt', 'us_pa_greene'))
    def test_validate_regions_one_all(self, _mock_modules):
        assert ingest_utils.validate_regions(['all']) == {
            'us_ny',
            'us_pa',
            'us_vt',
            'us_pa_greene',
        }

    @patch('pkgutil.iter_modules',
           return_value=fake_modules('us_ny', 'us_pa', 'us_vt', 'us_pa_greene'))
    def test_validate_regions_one_invalid(self, _mock_modules):
        assert not ingest_utils.validate_regions(['ca_bc'])

    @patch('pkgutil.iter_modules',
           return_value=fake_modules('us_ny', 'us_pa', 'us_vt', 'us_pa_greene'))
    def test_validate_regions_multiple_ok(self, _mock_modules):
        assert ingest_utils.validate_regions(['us_pa', 'us_ny']) == {'us_pa',
                                                                     'us_ny'}

    @patch('pkgutil.iter_modules',
           return_value=fake_modules('us_ny', 'us_pa', 'us_vt', 'us_pa_greene'))
    def test_validate_regions_multiple_invalid(self, _mock_modules):
        assert not ingest_utils.validate_regions(['us_pa', 'invalid'])

    @patch('pkgutil.iter_modules',
           return_value=fake_modules('us_ny', 'us_pa', 'us_vt', 'us_pa_greene'))
    @patch("recidiviz.utils.environment.get_gae_environment")
    @patch("recidiviz.utils.regions.get_region")
    def test_validate_regions_multiple_all(
            self, mock_region, mock_env, _mock_modules):
        fake_region = Mock()
        mock_region.return_value = fake_region
        fake_region.environment = 'production'
        mock_env.return_value = 'production'

        assert ingest_utils.validate_regions(['us_pa', 'all']) == {
            'us_ny',
            'us_pa',
            'us_vt',
            'us_pa_greene',
        }

    @patch('pkgutil.iter_modules',
           return_value=fake_modules('us_a', 'us_b', 'us_c', 'us_d'))
    @patch("recidiviz.utils.environment.get_gae_environment")
    @patch("recidiviz.utils.regions.get_region")
    def test_validate_regions_environments(
            self, mock_region, mock_env, _mock_modules):
        region_prod, region_staging, region_none = Mock(), Mock(), Mock()
        region_prod.environment = 'production'
        region_staging.environment = 'staging'
        region_none.environment = False

        mock_region.side_effect = [
            region_prod, region_none, region_prod, region_staging
        ]
        mock_env.return_value = 'production'

        assert len(ingest_utils.validate_regions(['all'])) == 2

    @patch('pkgutil.iter_modules',
           return_value=fake_modules('us_ny', 'us_pa', 'us_vt', 'us_pa_greene'))
    def test_validate_regions_multiple_all_invalid(self, _mock_modules):
        assert not ingest_utils.validate_regions(['all', 'invalid'])

    @patch('pkgutil.iter_modules',
           return_value=fake_modules('us_ny', 'us_pa', 'us_vt', 'us_pa_greene'))
    def test_validate_regions_empty(self, _mock_modules):
        assert ingest_utils.validate_regions([]) == set()

    def test_validate_scrape_types_one_ok(self):
        assert ingest_utils.validate_scrape_types(
            [constants.ScrapeType.SNAPSHOT.value]) == \
               [constants.ScrapeType.SNAPSHOT]

    def test_validate_scrape_types_one_all(self):
        assert ingest_utils.validate_scrape_types(['all']) == [
            constants.ScrapeType.BACKGROUND, constants.ScrapeType.SNAPSHOT]

    def test_validate_scrape_types_one_invalid(self):
        assert not ingest_utils.validate_scrape_types(['When You Were Young'])

    def test_validate_scrape_types_multiple_ok(self):
        assert ingest_utils.validate_scrape_types(
            [constants.ScrapeType.BACKGROUND.value,
             constants.ScrapeType.SNAPSHOT.value]) == \
               [constants.ScrapeType.BACKGROUND, constants.ScrapeType.SNAPSHOT]

    def test_validate_scrape_types_multiple_invalid(self):
        assert not ingest_utils.validate_scrape_types(
            [constants.ScrapeType.BACKGROUND.value, 'invalid'])

    def test_validate_scrape_types_multiple_all(self):
        assert ingest_utils.validate_scrape_types(
            [constants.ScrapeType.BACKGROUND.value, 'all']) == \
               [constants.ScrapeType.BACKGROUND, constants.ScrapeType.SNAPSHOT]

    def test_validate_scrape_types_multiple_all_invalid(self):
        assert not ingest_utils.validate_scrape_types(['all', 'invalid'])

    def test_validate_scrape_types_empty(self):
        assert ingest_utils.validate_scrape_types(
            []) == [constants.ScrapeType.BACKGROUND]

    @patch("recidiviz.common.common_utils.create_generated_id")
    def test_convert_ingest_info_id_is_generated(self, mock_create):
        mock_create.side_effect = self._create_generated_id
        info = ingest_info.IngestInfo()
        person = info.create_person()
        person.surname = 'testname'
        person.create_booking()

        expected_proto = ingest_info_pb2.IngestInfo()
        proto_person = expected_proto.people.add()
        proto_person.surname = 'testname'
        proto_person.person_id = '1_GENERATE'
        proto_booking = expected_proto.bookings.add()
        proto_booking.booking_id = '2_GENERATE'
        proto_person.booking_ids.append(proto_booking.booking_id)

        proto = ingest_utils.convert_ingest_info_to_proto(info)
        assert proto == expected_proto

        info_back = ingest_utils.convert_proto_to_ingest_info(proto)
        assert info_back == info

    def test_convert_ingest_info_id_is_not_generated(self):
        info = ingest_info.IngestInfo()
        person = info.create_person()
        person.person_id = 'id1'
        person.surname = 'testname'
        booking = person.create_booking()
        booking.booking_id = 'id2'
        booking.admission_date = 'testdate'

        expected_proto = ingest_info_pb2.IngestInfo()
        person = expected_proto.people.add()
        person.person_id = 'id1'
        person.surname = 'testname'
        person.booking_ids.append('id2')
        booking = expected_proto.bookings.add()
        booking.booking_id = 'id2'
        booking.admission_date = 'testdate'

        proto = ingest_utils.convert_ingest_info_to_proto(info)
        assert expected_proto == proto

        info_back = ingest_utils.convert_proto_to_ingest_info(proto)
        assert info_back == info

    @patch("recidiviz.common.common_utils.create_generated_id")
    def test_convert_ingest_info_one_charge_to_one_bond(self, mock_create):
        mock_create.side_effect = self._create_generated_id
        info = ingest_info.IngestInfo()
        person = info.create_person()
        person.person_id = 'id1'

        booking = person.create_booking()
        booking.booking_id = 'id1'
        charge = booking.create_charge()
        charge.charge_id = 'id1'
        bond1 = charge.create_bond()
        bond1.amount = '$1'
        charge = booking.create_charge()
        charge.charge_id = 'id2'
        bond2 = charge.create_bond()
        bond2.amount = '$1'

        expected_proto = ingest_info_pb2.IngestInfo()
        person = expected_proto.people.add()
        person.person_id = 'id1'
        person.booking_ids.append('id1')
        booking = expected_proto.bookings.add()
        booking.booking_id = 'id1'
        booking.charge_ids.extend(['id1', 'id2'])
        charge = expected_proto.charges.add()
        charge.charge_id = 'id1'
        proto_bond1 = expected_proto.bonds.add()
        proto_bond1.amount = '$1'
        proto_bond1.bond_id = '1_GENERATE'
        charge.bond_id = proto_bond1.bond_id
        charge = expected_proto.charges.add()
        charge.charge_id = 'id2'
        proto_bond2 = expected_proto.bonds.add()
        proto_bond2.amount = '$1'
        proto_bond2.bond_id = '2_GENERATE'
        charge.bond_id = proto_bond2.bond_id

        proto = ingest_utils.convert_ingest_info_to_proto(info)
        assert expected_proto == proto

        info_back = ingest_utils.convert_proto_to_ingest_info(proto)
        assert info_back == info

    @patch("recidiviz.common.common_utils.create_generated_id")
    def test_convert_ingest_info_many_charge_to_one_bond(self, mock_create):
        mock_create.side_effect = self._create_generated_id
        info = ingest_info.IngestInfo()
        person = info.create_person()
        person.person_id = 'id1'

        booking = person.create_booking()
        booking.booking_id = 'id1'
        charge = booking.create_charge()
        charge.charge_id = 'id1'
        bond1 = charge.create_bond()
        bond1.amount = '$1'
        charge = booking.create_charge()
        charge.charge_id = 'id2'
        charge.bond = bond1

        expected_proto = ingest_info_pb2.IngestInfo()
        person = expected_proto.people.add()
        person.person_id = 'id1'
        person.booking_ids.append('id1')
        booking = expected_proto.bookings.add()
        booking.booking_id = 'id1'
        booking.charge_ids.extend(['id1', 'id2'])
        charge = expected_proto.charges.add()
        charge.charge_id = 'id1'
        proto_bond = expected_proto.bonds.add()
        proto_bond.amount = '$1'
        proto_bond.bond_id = '1_GENERATE'
        charge.bond_id = proto_bond.bond_id
        charge = expected_proto.charges.add()
        charge.charge_id = 'id2'
        charge.bond_id = proto_bond.bond_id

        proto = ingest_utils.convert_ingest_info_to_proto(info)
        assert len(proto.bonds) == 1
        assert expected_proto == proto

        info_back = ingest_utils.convert_proto_to_ingest_info(proto)
        assert info_back == info

    def test_serializable(self):
        info = ingest_info.IngestInfo()
        person = info.create_person()
        person.person_id = 'id1'

        booking = person.create_booking()
        booking.booking_id = 'id1'
        charge = booking.create_charge()
        charge.charge_id = 'id1'
        bond1 = charge.create_bond()
        bond1.amount = '$1'
        charge = booking.create_charge()
        charge.charge_id = 'id2'
        bond2 = charge.create_bond()
        bond2.amount = '$1'

        converted_info = ingest_utils.ingest_info_from_serializable(
            ingest_utils.ingest_info_to_serializable(info))

        assert converted_info == info

    def test_convert_ingest_info_state_entities(self):
        # Arrange Python ingest info
        info = ingest_info.IngestInfo()
        person = info.create_state_person()
        person.state_person_id = 'person1'
        person.surname = 'testname'

        race = person.create_state_person_race()
        race.state_person_race_id = 'race1'
        race.race = 'white'
        ethnicity = person.create_state_person_ethnicity()
        ethnicity.state_person_ethnicity_id = 'ethnicity1'
        ethnicity.ethnicity = 'non-hispanic'
        external_id = person.create_state_person_external_id()
        external_id.state_person_external_id_id = 'external_id1'
        external_id.id_type = 'contrived'
        alias = person.create_state_alias()
        alias.state_alias_id = 'alias1'
        alias.surname = 'testerson'
        assessment = person.create_state_assessment()
        assessment.state_assessment_id = 'assessment1'
        assessment.assessment_score = '42'

        assessment_agent = assessment.create_state_agent()
        assessment_agent.state_agent_id = 'agent1'
        assessment_agent.full_name = 'Officer Jones'

        group = person.create_state_sentence_group()
        group.state_sentence_group_id = 'group1'

        fine = group.create_state_fine()
        fine.state_fine_id = 'fine1'

        incarceration_sentence = group.create_state_incarceration_sentence()
        incarceration_sentence.state_incarceration_sentence_id = 'is1'
        charge1 = incarceration_sentence.create_state_charge()
        charge1.state_charge_id = 'charge1'
        charge1.classification_type = 'F'
        incarceration_period = incarceration_sentence. \
            create_state_incarceration_period()
        incarceration_period.state_incarceration_period_id = 'ip1'
        incarceration_period.status = 'IN_CUSTODY'
        incident = incarceration_period.create_state_incarceration_incident()
        incident.state_incarceration_incident_id = 'incident1'
        incident.incident_type = 'FISTICUFFS'
        incident_outcome = \
            incident.create_state_incarceration_incident_outcome()
        incident_outcome.state_incarceration_incident_outcome_id = 'incident1-1'
        incident_outcome.outcome_type = 'FINE'

        incident_agent = incident.create_state_agent()
        incident_agent.state_agent_id = 'agent2'
        incident_agent.full_name = 'Officer Thompson'

        decision = incarceration_period.create_state_parole_decision()
        decision.state_parole_decision_id = 'decision1'

        decision_agent = decision.create_state_agent()
        decision_agent.state_agent_id = 'agent3'
        decision_agent.full_name = 'Officer Barkley'

        supervision_sentence = group.create_state_supervision_sentence()
        supervision_sentence.state_supervision_sentence_id = 'ss1'
        charge2 = supervision_sentence.create_state_charge()
        charge2.state_charge_id = 'charge2'
        charge2.classification_type = 'M'
        supervision_period = supervision_sentence. \
            create_state_supervision_period()
        supervision_period.state_supervision_period_id = 'sp1'
        supervision_period.status = 'TERMINATED'
        violation = supervision_period.create_state_supervision_violation()
        violation.state_supervision_violation_id = 'violation1'
        violation.is_violent = 'false'
        response = violation.create_state_supervision_violation_response()
        response.state_supervision_violation_response_id = 'response1'

        bond = charge1.create_state_bond()
        bond.state_bond_id = 'bond1'

        court_case = charge2.create_state_court_case()
        court_case.state_court_case_id = 'case1'

        court_case_agent = court_case.create_state_agent()
        court_case_agent.state_agent_id = 'agentJ'
        court_case_agent.full_name = 'Judge Agent'

        # Arrange Proto ingest info
        expected_proto = ingest_info_pb2.IngestInfo()
        person = expected_proto.state_people.add()
        person.state_person_id = 'person1'
        person.surname = 'testname'

        person.state_person_race_ids.append('race1')
        race = expected_proto.state_person_races.add()
        race.state_person_race_id = 'race1'
        race.race = 'white'
        person.state_person_ethnicity_ids.append('ethnicity1')
        ethnicity = expected_proto.state_person_ethnicities.add()
        ethnicity.state_person_ethnicity_id = 'ethnicity1'
        ethnicity.ethnicity = 'non-hispanic'
        person.state_person_external_ids_ids.append('external_id1')
        external_id = expected_proto.state_person_external_ids.add()
        external_id.state_person_external_id_id = 'external_id1'
        external_id.id_type = 'contrived'
        person.state_alias_ids.append('alias1')
        alias = expected_proto.state_aliases.add()
        alias.state_alias_id = 'alias1'
        alias.surname = 'testerson'
        person.state_assessment_ids.append('assessment1')
        assessment = expected_proto.state_assessments.add()
        assessment.state_assessment_id = 'assessment1'
        assessment.assessment_score = '42'

        assessment.conducting_agent_id = 'agent1'
        assessment_agent = expected_proto.state_agents.add()
        assessment_agent.state_agent_id = 'agent1'
        assessment_agent.full_name = 'Officer Jones'

        person.state_sentence_group_ids.append('group1')
        group = expected_proto.state_sentence_groups.add()
        group.state_sentence_group_id = 'group1'

        group.state_fine_ids.append('fine1')
        fine = expected_proto.state_fines.add()
        fine.state_fine_id = 'fine1'

        group.state_supervision_sentence_ids.append('ss1')
        supervision_sentence = expected_proto.state_supervision_sentences.add()
        supervision_sentence.state_supervision_sentence_id = 'ss1'
        supervision_sentence.state_charge_ids.append('charge2')
        charge2 = expected_proto.state_charges.add()
        charge2.state_charge_id = 'charge2'
        charge2.classification_type = 'M'
        supervision_sentence.state_supervision_period_ids.append('sp1')
        supervision_period = expected_proto.state_supervision_periods.add()
        supervision_period.state_supervision_period_id = 'sp1'
        supervision_period.status = 'TERMINATED'
        supervision_period.state_supervision_violation_ids.append('violation1')
        violation = expected_proto.state_supervision_violations.add()
        violation.state_supervision_violation_id = 'violation1'
        violation.is_violent = 'false'
        violation.state_supervision_violation_response_ids.append('response1')
        response = expected_proto.state_supervision_violation_responses.add()
        response.state_supervision_violation_response_id = 'response1'

        group.state_incarceration_sentence_ids.append('is1')
        incarceration_sentence = expected_proto.state_incarceration_sentences \
            .add()
        incarceration_sentence.state_incarceration_sentence_id = 'is1'
        incarceration_sentence.state_charge_ids.append('charge1')
        charge1 = expected_proto.state_charges.add()
        charge1.state_charge_id = 'charge1'
        charge1.classification_type = 'F'
        incarceration_sentence.state_incarceration_period_ids.append('ip1')
        incarceration_period = expected_proto.state_incarceration_periods.add()
        incarceration_period.state_incarceration_period_id = 'ip1'
        incarceration_period.status = 'IN_CUSTODY'
        incarceration_period.state_incarceration_incident_ids \
            .append('incident1')
        incident = expected_proto.state_incarceration_incidents.add()
        incident.state_incarceration_incident_id = 'incident1'
        incident.incident_type = 'FISTICUFFS'

        # An ordering requirement in the proto equality check at the end of this
        # test requires that this agent be added after agent1 and before agent2
        court_case_agent = expected_proto.state_agents.add()
        court_case_agent.state_agent_id = 'agentJ'
        court_case_agent.full_name = 'Judge Agent'

        incident.responding_officer_id = 'agent2'
        incident_agent = expected_proto.state_agents.add()
        incident_agent.state_agent_id = 'agent2'
        incident_agent.full_name = 'Officer Thompson'

        incident.state_incarceration_incident_outcome_ids.append('incident1-1')
        incident_outcome = \
            expected_proto.state_incarceration_incident_outcomes.add()
        incident_outcome.state_incarceration_incident_outcome_id = 'incident1-1'
        incident_outcome.outcome_type = 'FINE'

        incarceration_period.state_parole_decision_ids.append('decision1')
        decision = expected_proto.state_parole_decisions.add()
        decision.state_parole_decision_id = 'decision1'

        decision.decision_agent_ids.append('agent3')
        decision_agent = expected_proto.state_agents.add()
        decision_agent.state_agent_id = 'agent3'
        decision_agent.full_name = 'Officer Barkley'

        charge1.state_bond_id = 'bond1'
        bond = expected_proto.state_bonds.add()
        bond.state_bond_id = 'bond1'

        charge2.state_court_case_id = 'case1'
        court_case = expected_proto.state_court_cases.add()
        court_case.state_court_case_id = 'case1'

        court_case.judge_id = 'agentJ'

        # Act & Assert

        proto = ingest_utils.convert_ingest_info_to_proto(info)
        assert expected_proto == proto

        info_back = ingest_utils.convert_proto_to_ingest_info(proto)
        assert info_back == info

        # Assert that none of the proto's collections are empty, i.e. we've
        # tested all of the object graph
        proto_classes = [field.name for field in proto.DESCRIPTOR.fields]
        for cls in proto_classes:
            if cls.startswith('state_'):
                assert proto.__getattribute__(cls)

    def test_convert_ingest_info_duplicate_incarceration_incidents(self):
        # Arrange Python ingest info
        info = ingest_info.IngestInfo()
        person = info.create_state_person()
        person.state_person_id = 'person1'
        person.surname = 'testname'

        group = person.create_state_sentence_group()
        group.state_sentence_group_id = 'group1'

        incarceration_sentence = group.create_state_incarceration_sentence()
        incarceration_sentence.state_incarceration_sentence_id = 'is1'
        incarceration_period = incarceration_sentence. \
            create_state_incarceration_period()
        incarceration_period.state_incarceration_period_id = 'ip1'
        incarceration_period.status = 'IN_CUSTODY'

        incident = incarceration_period.create_state_incarceration_incident()
        incident.state_incarceration_incident_id = 'incident1'
        incident.incident_type = 'FISTICUFFS'
        incident_outcome = \
            incident.create_state_incarceration_incident_outcome()
        incident_outcome.state_incarceration_incident_outcome_id = 'incident1-1'
        incident_outcome.outcome_type = 'FINE'

        incident_dup = \
            incarceration_period.create_state_incarceration_incident()
        incident_dup.state_incarceration_incident_id = 'incident1'
        incident_dup.incident_type = 'FISTICUFFS'
        incident_outcome_2 = \
            incident_dup.create_state_incarceration_incident_outcome()
        incident_outcome_2.state_incarceration_incident_outcome_id = \
            'incident1-2'
        incident_outcome_2.outcome_type = 'FINE'

        # Arrange Proto ingest info
        expected_proto = ingest_info_pb2.IngestInfo()
        pb_person = expected_proto.state_people.add()
        pb_person.state_person_id = 'person1'
        pb_person.surname = 'testname'
        pb_person.state_sentence_group_ids.append('group1')
        pb_group = expected_proto.state_sentence_groups.add()
        pb_group.state_sentence_group_id = 'group1'
        pb_group.state_incarceration_sentence_ids.append('is1')

        pb_incarceration_sentence = \
            expected_proto.state_incarceration_sentences.add()
        pb_incarceration_sentence.state_incarceration_sentence_id = 'is1'
        pb_incarceration_sentence.state_incarceration_period_ids.append('ip1')
        pb_incarceration_period = \
            expected_proto.state_incarceration_periods.add()
        pb_incarceration_period.state_incarceration_period_id = 'ip1'
        pb_incarceration_period.status = 'IN_CUSTODY'
        pb_incarceration_period.state_incarceration_incident_ids \
            .append('incident1')

        pb_incident = expected_proto.state_incarceration_incidents.add()
        pb_incident.state_incarceration_incident_id = 'incident1'
        pb_incident.incident_type = 'FISTICUFFS'

        pb_incident.state_incarceration_incident_outcome_ids.append(
            'incident1-1')
        pb_incident_outcome = \
            expected_proto.state_incarceration_incident_outcomes.add()
        pb_incident_outcome.state_incarceration_incident_outcome_id = \
            'incident1-1'
        pb_incident_outcome.outcome_type = 'FINE'

        pb_incident.state_incarceration_incident_outcome_ids.append(
            'incident1-2')
        pb_incident_outcome = \
            expected_proto.state_incarceration_incident_outcomes.add()
        pb_incident_outcome.state_incarceration_incident_outcome_id = \
            'incident1-2'
        pb_incident_outcome.outcome_type = 'FINE'

        # Act & Assert
        proto = ingest_utils.convert_ingest_info_to_proto(info)
        assert expected_proto == proto

        # Duplicate IncarcerationIncident is gone.
        info_back = ingest_utils.convert_proto_to_ingest_info(proto)
        incarceration_period.state_incarceration_incidents = [incident]
        incident.state_incarceration_incident_outcomes = [
            incident_outcome, incident_outcome_2]

        assert info_back == info
