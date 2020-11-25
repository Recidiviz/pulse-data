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
"""US_ND specific entity matching tests."""
import datetime

import attr
from mock import create_autospec, patch

from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_incarceration import \
    StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodStatus, StateIncarcerationPeriodAdmissionReason, \
    StateIncarcerationPeriodReleaseReason
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_period import \
    StateSupervisionPeriodStatus
from recidiviz.common.constants.state.state_supervision_violation_response \
    import StateSupervisionViolationResponseRevocationType
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity.state.entities import \
    StatePersonExternalId, StatePerson, \
    StateIncarcerationPeriod, StateIncarcerationSentence, \
    StateSupervisionSentence, StateSupervisionViolationResponse, \
    StateSupervisionViolation, StateSupervisionPeriod, StateSentenceGroup, \
    StateAgent, StateSupervisionCaseTypeEntry
from recidiviz.persistence.entity_matching import entity_matching
from recidiviz.tests.persistence.database.schema.state.schema_test_utils \
    import generate_person, generate_external_id, \
    generate_incarceration_sentence, \
    generate_sentence_group, \
    generate_incarceration_period, \
    generate_supervision_period, \
    generate_supervision_sentence, generate_agent, generate_supervision_case_type_entry
from recidiviz.tests.persistence.entity_matching.state.\
    base_state_entity_matcher_test_classes import BaseStateEntityMatcherTest
from recidiviz.utils.regions import Region

_EXTERNAL_ID = 'EXTERNAL_ID-1'
_EXTERNAL_ID_2 = 'EXTERNAL_ID-2'
_EXTERNAL_ID_3 = 'EXTERNAL_ID-3'
_EXTERNAL_ID_4 = 'EXTERNAL_ID-4'
_ID_TYPE = 'ID_TYPE'
_FULL_NAME = 'FULL_NAME'
_COUNTY_CODE = 'Iredell'
_US_ND = 'US_ND'
_OTHER_STATE_CODE = 'NC'
_FACILITY = 'FACILITY'
_FACILITY_2 = 'FACILITY_2'
_DATE_1 = datetime.date(year=2019, month=1, day=1)
_DATE_2 = datetime.date(year=2019, month=2, day=1)
_DATE_3 = datetime.date(year=2019, month=3, day=1)
_DATE_4 = datetime.date(year=2019, month=4, day=1)
_DATE_5 = datetime.date(year=2019, month=5, day=1)
_DATE_6 = datetime.date(year=2019, month=6, day=1)


class TestNdEntityMatching(BaseStateEntityMatcherTest):
    """Test class for US_ND specific entity matching logic."""

    def get_fake_region(self, **_kwargs):
        fake_region = create_autospec(Region)
        overrides_builder = EnumOverrides.Builder()
        overrides_builder.add(
            'ADM', StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION)
        fake_region.get_enum_overrides.return_value = overrides_builder.build()
        return fake_region

    def test_runMatch_checkPlaceholdersWhenNoRootEntityMatch(self) -> None:
        """Tests that ingested people are matched with DB placeholder people
        when a root entity match doesn't exist. Specific to US_ND.
        """
        db_placeholder_person = generate_person(state_code=_US_ND)
        db_sg = generate_sentence_group(
            state_code=_US_ND, external_id=_EXTERNAL_ID)
        db_placeholder_person.sentence_groups = [db_sg]
        db_placeholder_person_other_state = generate_person(state_code=_US_ND)
        db_sg_other_state = generate_sentence_group(
            state_code=_OTHER_STATE_CODE,
            external_id=_EXTERNAL_ID)
        db_placeholder_person_other_state.sentence_groups = [db_sg_other_state]

        self._commit_to_db(
            db_placeholder_person, db_placeholder_person_other_state)

        sg = attr.evolve(
            self.to_entity(db_sg), sentence_group_id=None)
        external_id = StatePersonExternalId.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_US_ND,
            id_type=_ID_TYPE)
        person = attr.evolve(
            self.to_entity(db_placeholder_person), person_id=None,
            full_name=_FULL_NAME,
            external_ids=[external_id],
            sentence_groups=[sg])

        expected_sg = attr.evolve(self.to_entity(db_sg))
        expected_external_id = attr.evolve(external_id)
        expected_person = attr.evolve(
            person,
            external_ids=[expected_external_id],
            sentence_groups=[expected_sg])
        expected_placeholder_person = attr.evolve(
            self.to_entity(db_placeholder_person), sentence_groups=[])
        expected_placeholder_person_other_state = self.to_entity(
            db_placeholder_person_other_state)

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _US_ND, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person, expected_placeholder_person,
             expected_placeholder_person_other_state],
            matched_entities.people, session)
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_runMatch_sameEntities_noDuplicates(self) -> None:
        db_placeholder_person = generate_person(state_code=_US_ND)
        db_is = generate_incarceration_sentence(
            person=db_placeholder_person,
            state_code=_US_ND,
            date_imposed=_DATE_1)
        db_sg = generate_sentence_group(
            external_id=_EXTERNAL_ID,
            state_code=_US_ND,
            incarceration_sentences=[db_is])
        db_placeholder_person.sentence_groups = [db_sg]

        self._commit_to_db(db_placeholder_person)

        inc_s = attr.evolve(self.to_entity(db_is),
                            state_code=_US_ND,
                            incarceration_sentence_id=None)
        sg = attr.evolve(
            self.to_entity(db_sg), sentence_group_id=None,
            state_code=_US_ND,
            incarceration_sentences=[inc_s])
        placeholder_person = attr.evolve(
            self.to_entity(db_placeholder_person), person_id=None,
            sentence_groups=[sg])

        expected_is = attr.evolve(self.to_entity(db_is))
        expected_sg = attr.evolve(self.to_entity(db_sg),
                                  incarceration_sentences=[expected_is])
        expected_placeholder_person = attr.evolve(
            self.to_entity(db_placeholder_person),
            sentence_groups=[expected_sg])

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _US_ND, ingested_people=[placeholder_person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_placeholder_person], matched_entities.people, session)
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_runMatch_associateSvrsToIps(self) -> None:
        # Arrange 1 - Match
        db_placeholder_person = generate_person(state_code=_US_ND)
        db_ip_1 = generate_incarceration_period(
            person=db_placeholder_person,
            state_code=_US_ND,
            admission_date=_DATE_2,
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION.value)
        db_ip_2 = generate_incarceration_period(
            person=db_placeholder_person,
            state_code=_US_ND,
            admission_date=_DATE_3,
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION.value)
        db_placeholder_is = generate_incarceration_sentence(
            person=db_placeholder_person,
            state_code=_US_ND,
            incarceration_periods=[db_ip_1, db_ip_2])

        db_sg = generate_sentence_group(
            external_id=_EXTERNAL_ID,
            state_code=_US_ND,
            incarceration_sentences=[db_placeholder_is],
        )

        db_placeholder_person.sentence_groups = [db_sg]

        self._commit_to_db(db_placeholder_person)

        svr_1 = StateSupervisionViolationResponse.new_with_defaults(
            state_code=_US_ND,
            response_date=_DATE_2 + datetime.timedelta(days=1),
            revocation_type=
            StateSupervisionViolationResponseRevocationType.REINCARCERATION)
        svr_2 = StateSupervisionViolationResponse.new_with_defaults(
            state_code=_US_ND,
            response_date=_DATE_3 - datetime.timedelta(days=1),
            revocation_type=
            StateSupervisionViolationResponseRevocationType.REINCARCERATION)
        placeholder_sv = StateSupervisionViolation.new_with_defaults(
            state_code=_US_ND,
            supervision_violation_responses=[svr_1, svr_2])
        placeholder_sp = StateSupervisionPeriod.new_with_defaults(
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
            state_code=_US_ND,
            supervision_violation_entries=[placeholder_sv])
        placeholder_ss = StateSupervisionSentence.new_with_defaults(
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            state_code=_US_ND,
            supervision_periods=[placeholder_sp])
        sg = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_US_ND,
            supervision_sentences=[placeholder_ss])
        placeholder_person = StatePerson.new_with_defaults(sentence_groups=[sg], state_code=_US_ND)

        expected_svr_1 = attr.evolve(svr_1)
        expected_svr_2 = attr.evolve(svr_2)
        expected_placeholder_sv = attr.evolve(
            placeholder_sv,
            supervision_violation_responses=[expected_svr_1, expected_svr_2])
        expected_placeholder_sp = attr.evolve(
            placeholder_sp,
            supervision_violation_entries=[expected_placeholder_sv])
        expected_placeholder_ss = attr.evolve(
            placeholder_ss, supervision_periods=[expected_placeholder_sp])

        expected_ip_1 = attr.evolve(
            self.to_entity(db_ip_1),
            source_supervision_violation_response=expected_svr_1)
        expected_ip_2 = attr.evolve(
            self.to_entity(db_ip_2),
            source_supervision_violation_response=expected_svr_2)

        expected_placeholder_is = attr.evolve(
            self.to_entity(db_placeholder_is),
            incarceration_periods=[expected_ip_1, expected_ip_2])
        expected_placeholder_sg = attr.evolve(
            self.to_entity(db_sg),
            supervision_sentences=[expected_placeholder_ss],
            incarceration_sentences=[expected_placeholder_is])
        expected_placeholder_person = attr.evolve(
            placeholder_person, person_id=db_placeholder_person.person_id,
            sentence_groups=[expected_placeholder_sg])

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _US_ND, ingested_people=[placeholder_person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_placeholder_person], matched_entities.people, session)
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_match_mergeIncomingIncarcerationPeriods(self) -> None:
        # Arrange 1 - Match
        db_person = schema.StatePerson(full_name=_FULL_NAME, state_code=_US_ND)
        db_incarceration_sentence = \
            schema.StateIncarcerationSentence(
                state_code=_US_ND,
                status=StateIncarcerationPeriodStatus.EXTERNAL_UNKNOWN.value,
                external_id=_EXTERNAL_ID,
                person=db_person)
        db_sentence_group = schema.StateSentenceGroup(
            state_code=_US_ND,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[db_incarceration_sentence],
            person=db_person)
        db_external_id = schema.StatePersonExternalId(
            state_code=_US_ND,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID,
            person=db_person)
        db_person.sentence_groups = [db_sentence_group]
        db_person.external_ids = [db_external_id]

        self._commit_to_db(db_person)

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            state_code=_US_ND,
            external_id=_EXTERNAL_ID,
            facility=_FACILITY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            admission_date=datetime.date(year=2019, month=1, day=1),
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION)
        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_US_ND,
            external_id=_EXTERNAL_ID_2,
            facility=_FACILITY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            release_date=datetime.date(year=2019, month=1, day=2),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)
        incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_US_ND,
            external_id=_EXTERNAL_ID_3,
            facility=_FACILITY_2,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            admission_date=datetime.date(year=2019, month=1, day=2),
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.TRANSFER)
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID,
            incarceration_periods=[incarceration_period,
                                   incarceration_period_2,
                                   incarceration_period_3])
        sentence_group = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_US_ND,
            incarceration_sentences=[incarceration_sentence])
        placeholder_person = StatePerson.new_with_defaults(
            sentence_groups=[sentence_group], state_code=_US_ND)

        expected_person = StatePerson.new_with_defaults(
            full_name=_FULL_NAME, state_code=_US_ND)
        expected_complete_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                external_id=_EXTERNAL_ID + '|' + _EXTERNAL_ID_2,
                state_code=_US_ND,
                facility=_FACILITY,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                admission_date=datetime.date(year=2019, month=1, day=1),
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                release_date=datetime.date(year=2019, month=1, day=2),
                release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)
        expected_incomplete_incarceration_period = attr.evolve(
            incarceration_period_3)
        expected_incarceration_sentence = \
            StateIncarcerationSentence.new_with_defaults(
                incarceration_sentence_id=db_incarceration_sentence.incarceration_sentence_id,
                status=StateSentenceStatus.EXTERNAL_UNKNOWN,
                state_code=_US_ND,
                external_id=_EXTERNAL_ID,
                incarceration_periods=[
                    expected_complete_incarceration_period,
                    expected_incomplete_incarceration_period])
        expected_sentence_group = StateSentenceGroup.new_with_defaults(
            status=StateSentenceStatus.EXTERNAL_UNKNOWN,
            state_code=_US_ND,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[expected_incarceration_sentence])
        expected_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=db_external_id.person_external_id_id, state_code=_US_ND,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        expected_person.external_ids = [expected_external_id]
        expected_person.sentence_groups = [expected_sentence_group]

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _US_ND, ingested_people=[placeholder_person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session)
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_mergeIncompleteIncarcerationPeriodOntoComplete(
            self) -> None:
        """Tests correct matching behavior when an incomplete period is ingested
        and a matching complete period is in the db.
        """
        # Arrange 1 - Match
        db_person = generate_person(full_name=_FULL_NAME, state_code=_US_ND)
        db_complete_incarceration_period = \
            generate_incarceration_period(
                person=db_person,
                state_code=_US_ND,
                external_id=_EXTERNAL_ID + '|' + _EXTERNAL_ID_2,
                facility=_FACILITY,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY.value,
                admission_date=datetime.date(year=2018, month=1, day=1),
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION.value,
                release_date=datetime.date(year=2018, month=1, day=2),
                release_reason=
                StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED.value)

        db_incarceration_sentence = \
            generate_incarceration_sentence(
                person=db_person,
                state_code=_US_ND,
                status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
                external_id=_EXTERNAL_ID,
                incarceration_periods=[db_complete_incarceration_period])
        db_sentence_group = generate_sentence_group(
            state_code=_US_ND,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[db_incarceration_sentence])
        db_external_id = generate_external_id(
            state_code=_US_ND,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        db_person.sentence_groups = [db_sentence_group]
        db_person.external_ids = [db_external_id]

        self._commit_to_db(db_person)

        incomplete_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                state_code=_US_ND,
                external_id=_EXTERNAL_ID_2,
                release_date=datetime.date(year=2018, month=1, day=2),
                release_reason=
                StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                county_code=_COUNTY_CODE)

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code=_US_ND,
            external_id=_EXTERNAL_ID,
            incarceration_periods=[incomplete_incarceration_period])
        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code=_US_ND,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[incarceration_sentence])
        placeholder_person = StatePerson.new_with_defaults(
            sentence_groups=[sentence_group], state_code=_US_ND)

        expected_complete_incarceration_period = attr.evolve(
            self.to_entity(db_complete_incarceration_period),
            county_code=_COUNTY_CODE)

        expected_incarceration_sentence = \
            StateIncarcerationSentence.new_with_defaults(
                incarceration_sentence_id=db_incarceration_sentence.incarceration_sentence_id,
                status=StateSentenceStatus.EXTERNAL_UNKNOWN,
                state_code=_US_ND,
                external_id=_EXTERNAL_ID,
                incarceration_periods=[
                    expected_complete_incarceration_period])
        expected_sentence_group = attr.evolve(
            self.to_entity(db_sentence_group),
            incarceration_sentences=[expected_incarceration_sentence])
        expected_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=db_external_id.person_external_id_id,
            state_code=_US_ND,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        expected_person = StatePerson.new_with_defaults(
            person_id=db_person.person_id, full_name=_FULL_NAME,
            external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group],
            state_code=_US_ND)

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _US_ND, ingested_people=[placeholder_person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session)
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_mergeCompleteIncarcerationPeriodOntoIncomplete(
            self) -> None:
        """Tests correct matching behavior when a complete period is ingested
        and a matching incomplete period is in the db.
        """
        # Arrange 1 - Match
        db_person = generate_person(full_name=_FULL_NAME, state_code=_US_ND)
        db_incomplete_incarceration_period = \
            generate_incarceration_period(
                person=db_person,
                state_code=_US_ND,
                external_id=_EXTERNAL_ID,
                facility=_FACILITY,
                incarceration_type=StateIncarcerationType.STATE_PRISON.value,
                status=StateIncarcerationPeriodStatus.IN_CUSTODY.value,
                admission_date=datetime.date(year=2019, month=1, day=1),
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION.value)

        db_incarceration_sentence = \
            generate_incarceration_sentence(
                person=db_person,
                state_code=_US_ND,
                status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
                external_id=_EXTERNAL_ID,
                incarceration_periods=[db_incomplete_incarceration_period])
        db_sentence_group = generate_sentence_group(
            state_code=_US_ND,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[db_incarceration_sentence])
        db_external_id = generate_external_id(
            state_code=_US_ND,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        db_person.sentence_groups = [db_sentence_group]
        db_person.external_ids = [db_external_id]

        self._commit_to_db(db_person)

        complete_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                state_code=_US_ND,
                external_id=_EXTERNAL_ID + '|' + _EXTERNAL_ID_2,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                facility=_FACILITY,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                admission_date=datetime.date(year=2019, month=1, day=1),
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                release_date=datetime.date(year=2019, month=1, day=2),
                release_reason=
                StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code=_US_ND,
            external_id=_EXTERNAL_ID,
            incarceration_periods=[complete_incarceration_period])
        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code=_US_ND,
            external_id=_EXTERNAL_ID,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN,
            incarceration_sentences=[incarceration_sentence])
        placeholder_person = StatePerson.new_with_defaults(
            sentence_groups=[sentence_group])

        expected_complete_incarceration_period = attr.evolve(
            complete_incarceration_period,
            incarceration_period_id=db_incomplete_incarceration_period.incarceration_period_id)

        expected_incarceration_sentence = \
            StateIncarcerationSentence.new_with_defaults(
                incarceration_sentence_id=db_incarceration_sentence.incarceration_sentence_id,
                status=StateSentenceStatus.EXTERNAL_UNKNOWN,
                state_code=_US_ND,
                external_id=_EXTERNAL_ID,
                incarceration_periods=[expected_complete_incarceration_period])
        expected_sentence_group = attr.evolve(
            self.to_entity(db_sentence_group),
            incarceration_sentences=[expected_incarceration_sentence])
        expected_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=db_external_id.person_external_id_id,
            state_code=_US_ND,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        expected_person = StatePerson.new_with_defaults(
            person_id=db_person.person_id, full_name=_FULL_NAME,
            external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group],
            state_code=_US_ND)

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _US_ND, ingested_people=[placeholder_person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session)
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_mergeCompleteIncarcerationPeriods(self) -> None:
        """Tests correct matching behavior when a complete period is ingested
        and a matching complete period is in the db.
        """
        # Arrange 1 - Match
        db_person = generate_person(full_name=_FULL_NAME, state_code=_US_ND)
        db_complete_incarceration_period = \
            generate_incarceration_period(
                person=db_person,
                state_code=_US_ND,
                external_id=_EXTERNAL_ID + '|' + _EXTERNAL_ID_2,
                facility=_FACILITY,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY.value,
                admission_date=datetime.date(year=2018, month=1, day=1),
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION.value,
                release_date=datetime.date(year=2018, month=1, day=2),
                release_reason=
                StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED.value)

        db_incarceration_sentence = \
            generate_incarceration_sentence(
                person=db_person,
                state_code=_US_ND,
                status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
                external_id=_EXTERNAL_ID,
                incarceration_periods=[db_complete_incarceration_period])
        db_sentence_group = generate_sentence_group(
            state_code=_US_ND,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[db_incarceration_sentence])
        db_external_id = generate_external_id(
            state_code=_US_ND,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        db_person.sentence_groups = [db_sentence_group]
        db_person.external_ids = [db_external_id]

        self._commit_to_db(db_person)

        updated_incarceration_period = attr.evolve(
            self.to_entity(db_complete_incarceration_period),
            incarceration_period_id=None,
            county_code=_COUNTY_CODE)
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_US_ND,
            incarceration_periods=[updated_incarceration_period])
        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code=_US_ND,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[incarceration_sentence])
        placeholder_person = StatePerson.new_with_defaults(
            sentence_groups=[sentence_group],
            state_code=_US_ND)

        expected_complete_incarceration_period = attr.evolve(
            updated_incarceration_period,
            incarceration_period_id=db_complete_incarceration_period.incarceration_period_id)

        expected_incarceration_sentence = \
            StateIncarcerationSentence.new_with_defaults(
                incarceration_sentence_id=db_incarceration_sentence.incarceration_sentence_id,
                status=StateSentenceStatus.EXTERNAL_UNKNOWN,
                state_code=_US_ND,
                external_id=_EXTERNAL_ID,
                incarceration_periods=[expected_complete_incarceration_period])
        expected_sentence_group = attr.evolve(
            self.to_entity(db_sentence_group),
            incarceration_sentences=[expected_incarceration_sentence])
        expected_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=db_external_id.person_external_id_id,
            state_code=_US_ND,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        expected_person = StatePerson.new_with_defaults(
            person_id=db_person.person_id, full_name=_FULL_NAME,
            external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group],
            state_code=_US_ND)

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _US_ND, ingested_people=[placeholder_person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session)
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_mergeIncompleteIncarcerationPeriods(self) -> None:
        """Tests correct matching behavior when an incomplete period is ingested
        and a matching incomplete period is in the db.
        """
        # Arrange 1 - Match
        db_person = generate_person(full_name=_FULL_NAME, state_code=_US_ND)
        db_incomplete_incarceration_period = \
            generate_incarceration_period(
                person=db_person,
                state_code=_US_ND,
                external_id=_EXTERNAL_ID_3,
                facility=_FACILITY,
                status=StateIncarcerationPeriodStatus.IN_CUSTODY.value,
                admission_date=datetime.date(year=2019, month=1, day=1),
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION.value)
        db_complete_incarceration_period = \
            generate_incarceration_period(
                person=db_person,
                state_code=_US_ND,
                external_id=_EXTERNAL_ID + '|' + _EXTERNAL_ID_2,
                facility=_FACILITY,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY.value,
                admission_date=datetime.date(year=2018, month=1, day=1),
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION.value,
                release_date=datetime.date(year=2018, month=1, day=2),
                release_reason=
                StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED.value)

        db_incarceration_sentence = \
            generate_incarceration_sentence(
                person=db_person,
                state_code=_US_ND,
                status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
                external_id=_EXTERNAL_ID,
                incarceration_periods=[
                    db_complete_incarceration_period,
                    db_incomplete_incarceration_period])
        db_sentence_group = generate_sentence_group(
            state_code=_US_ND,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[db_incarceration_sentence])
        db_external_id = generate_external_id(
            state_code=_US_ND,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        db_person.sentence_groups = [db_sentence_group]
        db_person.external_ids = [db_external_id]

        self._commit_to_db(db_person)

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            state_code=_US_ND,
            external_id=_EXTERNAL_ID_4,
            facility=_FACILITY,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            release_date=datetime.date(year=2019, month=1, day=2),
            release_reason=
            StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_US_ND,
            incarceration_periods=[incarceration_period])
        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code=_US_ND,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[incarceration_sentence])
        placeholder_person = StatePerson.new_with_defaults(
            sentence_groups=[sentence_group], state_code=_US_ND)

        expected_complete_incarceration_period = attr.evolve(
            self.to_entity(db_complete_incarceration_period))
        expected_new_complete_incarceration_period = attr.evolve(
            self.to_entity(db_incomplete_incarceration_period),
            external_id=_EXTERNAL_ID_3 + '|' + _EXTERNAL_ID_4,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            release_date=datetime.date(year=2019, month=1, day=2),
            release_reason=
            StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)

        expected_incarceration_sentence = \
            StateIncarcerationSentence.new_with_defaults(
                incarceration_sentence_id=db_incarceration_sentence.incarceration_sentence_id,
                status=StateSentenceStatus.EXTERNAL_UNKNOWN,
                state_code=_US_ND,
                external_id=_EXTERNAL_ID,
                incarceration_periods=[
                    expected_new_complete_incarceration_period,
                    expected_complete_incarceration_period])
        expected_sentence_group = attr.evolve(
            self.to_entity(db_sentence_group),
            incarceration_sentences=[expected_incarceration_sentence])
        expected_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=db_external_id.person_external_id_id,
            state_code=_US_ND,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        expected_person = StatePerson.new_with_defaults(
            person_id=db_person.person_id, full_name=_FULL_NAME,
            external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group],
            state_code=_US_ND)

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _US_ND, ingested_people=[placeholder_person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session)
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_dontMergePeriodsFromDifferentStates(self) -> None:
        """Tests that incarceration periods don't match when either doesn't have
        US_ND as its state code.
        """
        # Arrange 1 - Match
        db_person = generate_person(full_name=_FULL_NAME, state_code=_US_ND)
        db_incomplete_incarceration_period = \
            generate_incarceration_period(
                person=db_person,
                state_code=_US_ND,
                external_id=_EXTERNAL_ID_3,
                facility=_FACILITY,
                status=StateIncarcerationPeriodStatus.IN_CUSTODY.value,
                admission_date=datetime.date(year=2019, month=1, day=1),
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION.value)

        db_incarceration_sentence = \
            generate_incarceration_sentence(
                person=db_person,
                state_code=_US_ND,
                status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
                external_id=_EXTERNAL_ID,
                incarceration_periods=[db_incomplete_incarceration_period])
        db_sentence_group = generate_sentence_group(
            state_code=_US_ND,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[db_incarceration_sentence])
        db_external_id = generate_external_id(
            state_code=_US_ND,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        db_person.sentence_groups = [db_sentence_group]
        db_person.external_ids = [db_external_id]

        self._commit_to_db(db_person)

        incarceration_period_different_state = \
            StateIncarcerationPeriod.new_with_defaults(
                state_code=_OTHER_STATE_CODE,
                external_id=_EXTERNAL_ID_4,
                facility=_FACILITY,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                release_date=datetime.date(year=2019, month=1, day=2),
                release_reason=
                StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_US_ND,
            incarceration_periods=[incarceration_period_different_state])
        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code=_US_ND,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[incarceration_sentence])
        placeholder_person = StatePerson.new_with_defaults(
            sentence_groups=[sentence_group], state_code=_US_ND)

        expected_incarceration_period = attr.evolve(
            self.to_entity(db_incomplete_incarceration_period))
        expected_incarceration_period_different_state = attr.evolve(
            incarceration_period_different_state)

        expected_incarceration_sentence = \
            StateIncarcerationSentence.new_with_defaults(
                incarceration_sentence_id=db_incarceration_sentence.incarceration_sentence_id,
                status=StateSentenceStatus.EXTERNAL_UNKNOWN,
                state_code=_US_ND,
                external_id=_EXTERNAL_ID,
                incarceration_periods=[
                    expected_incarceration_period,
                    expected_incarceration_period_different_state])
        expected_sentence_group = attr.evolve(
            self.to_entity(db_sentence_group),
            incarceration_sentences=[expected_incarceration_sentence])
        expected_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=db_external_id.person_external_id_id,
            state_code=_US_ND,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        expected_person = StatePerson.new_with_defaults(
            person_id=db_person.person_id, full_name=_FULL_NAME,
            external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group],
            state_code=_US_ND)

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _US_ND, ingested_people=[placeholder_person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session)
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_temporaryCustodyPeriods(self) -> None:
        # Arrange 1 - Match
        db_person = generate_person(full_name=_FULL_NAME, state_code=_US_ND)
        db_incomplete_temporary_custody = \
            generate_incarceration_period(
                person=db_person,
                state_code=_US_ND,
                external_id=_EXTERNAL_ID,
                facility=_FACILITY,
                status=StateIncarcerationPeriodStatus.IN_CUSTODY.value,
                incarceration_type=StateIncarcerationType.COUNTY_JAIL.value,
                admission_date=datetime.date(year=2018, month=1, day=1),
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION.value,
                admission_reason_raw_text='ADM')

        db_incarceration_sentence = \
            generate_incarceration_sentence(
                person=db_person,
                state_code=_US_ND,
                status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
                external_id=_EXTERNAL_ID,
                incarceration_periods=[db_incomplete_temporary_custody])
        db_sentence_group = generate_sentence_group(
            state_code=_US_ND,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[db_incarceration_sentence])
        db_external_id = generate_external_id(
            state_code=_US_ND,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        db_person.sentence_groups = [db_sentence_group]
        db_person.external_ids = [db_external_id]

        self._commit_to_db(db_person)

        new_temporary_custody_period = \
            StateIncarcerationPeriod.new_with_defaults(
                state_code=_US_ND,
                external_id=_EXTERNAL_ID_2,
                facility=_FACILITY,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                incarceration_type=StateIncarcerationType.COUNTY_JAIL,
                release_date=datetime.date(year=2018, month=1, day=2),
                release_reason=
                StateIncarcerationPeriodAdmissionReason.TRANSFER)

        new_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            state_code=_US_ND,
            external_id=_EXTERNAL_ID_3,
            facility=_FACILITY_2,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=datetime.date(year=2018, month=1, day=3),
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.TRANSFER)
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_US_ND,
            incarceration_periods=[new_temporary_custody_period,
                                   new_incarceration_period])
        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code=_US_ND,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[incarceration_sentence])
        placeholder_person = StatePerson.new_with_defaults(
            sentence_groups=[sentence_group], state_code=_US_ND)

        expected_complete_temporary_custody = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=db_incomplete_temporary_custody.incarceration_period_id,
                state_code=_US_ND,
                external_id=_EXTERNAL_ID + '|' + _EXTERNAL_ID_2,
                facility=_FACILITY,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                incarceration_type=StateIncarcerationType.COUNTY_JAIL,
                admission_date=datetime.date(year=2018, month=1, day=1),
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
                admission_reason_raw_text='ADM',
                release_date=datetime.date(year=2018, month=1, day=2),
                release_reason=
                StateIncarcerationPeriodReleaseReason.
                RELEASED_FROM_TEMPORARY_CUSTODY)
        expected_new_period = attr.evolve(
            new_incarceration_period,
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION)

        expected_incarceration_sentence = \
            StateIncarcerationSentence.new_with_defaults(
                incarceration_sentence_id=db_incarceration_sentence.incarceration_sentence_id,
                status=StateSentenceStatus.EXTERNAL_UNKNOWN,
                state_code=_US_ND,
                external_id=_EXTERNAL_ID,
                incarceration_periods=[
                    expected_complete_temporary_custody, expected_new_period])
        expected_sentence_group = attr.evolve(
            self.to_entity(db_sentence_group),
            incarceration_sentences=[expected_incarceration_sentence])
        expected_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=db_external_id.person_external_id_id,
            state_code=_US_ND,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        expected_person = StatePerson.new_with_defaults(
            person_id=db_person.person_id, full_name=_FULL_NAME,
            external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group],
            state_code=_US_ND)

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _US_ND, ingested_people=[placeholder_person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session)
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_mergeIngestedAndDbIncarcerationPeriods_reverse(
            self) -> None:
        """Tests that periods are correctly merged when the release period is
        ingested before the admission period."""
        # Arrange 1 - Match
        db_person = generate_person(full_name=_FULL_NAME, state_code=_US_ND)
        db_incomplete_incarceration_period = generate_incarceration_period(
            person=db_person,
            state_code=_US_ND,
            external_id=_EXTERNAL_ID_4,
            facility=_FACILITY,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY.value,
            release_date=datetime.date(year=2019, month=1, day=2),
            release_reason=
            StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED.value)
        db_complete_incarceration_period = \
            generate_incarceration_period(
                person=db_person,
                state_code=_US_ND,
                external_id=_EXTERNAL_ID + '|' + _EXTERNAL_ID_2,
                facility=_FACILITY,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY.value,
                admission_date=datetime.date(year=2018, month=1, day=1),
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION.value,
                release_date=datetime.date(year=2018, month=1, day=2),
                release_reason=
                StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED.value)

        db_incarceration_sentence = \
            generate_incarceration_sentence(
                person=db_person,
                state_code=_US_ND,
                status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
                external_id=_EXTERNAL_ID,
                incarceration_periods=[
                    db_complete_incarceration_period,
                    db_incomplete_incarceration_period])
        db_sentence_group = generate_sentence_group(
            state_code=_US_ND,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[db_incarceration_sentence])
        db_external_id = generate_external_id(
            state_code=_US_ND,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        db_person.sentence_groups = [db_sentence_group]
        db_person.external_ids = [db_external_id]

        self._commit_to_db(db_person)

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            state_code=_US_ND,
            external_id=_EXTERNAL_ID_3,
            facility=_FACILITY,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            admission_date=datetime.date(year=2019, month=1, day=1),
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION)
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code=_US_ND,
            external_id=_EXTERNAL_ID,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_periods=[incarceration_period])
        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code=_US_ND,
            external_id=_EXTERNAL_ID,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_sentences=[incarceration_sentence])
        placeholder_person = StatePerson.new_with_defaults(
            sentence_groups=[sentence_group], state_code=_US_ND)

        expected_complete_incarceration_period = attr.evolve(
            self.to_entity(db_complete_incarceration_period))
        expected_new_complete_incarceration_period = attr.evolve(
            self.to_entity(db_incomplete_incarceration_period),
            external_id=_EXTERNAL_ID_3 + '|' + _EXTERNAL_ID_4,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=datetime.date(year=2019, month=1, day=1),
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION)

        expected_incarceration_sentence = \
            StateIncarcerationSentence.new_with_defaults(
                incarceration_sentence_id=db_incarceration_sentence.incarceration_sentence_id,
                status=StateSentenceStatus.EXTERNAL_UNKNOWN,
                state_code=_US_ND,
                external_id=_EXTERNAL_ID,
                incarceration_periods=[
                    expected_new_complete_incarceration_period,
                    expected_complete_incarceration_period])
        expected_sentence_group = attr.evolve(
            self.to_entity(db_sentence_group),
            incarceration_sentences=[expected_incarceration_sentence])
        expected_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=db_external_id.person_external_id_id,
            state_code=_US_ND,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        expected_person = StatePerson.new_with_defaults(
            person_id=db_person.person_id, full_name=_FULL_NAME,
            external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group],
            state_code=_US_ND)

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _US_ND, ingested_people=[placeholder_person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session)
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

    @patch("recidiviz.utils.environment.get_gae_environment")
    def test_runMatch_moveSupervisingOfficerOntoOpenSupervisionPeriods(self, mock_environment) -> None:
        mock_environment.return_value = 'production'
        db_supervising_officer = generate_agent(
            external_id=_EXTERNAL_ID, state_code=_US_ND)
        db_person = generate_person(state_code=_US_ND)
        db_external_id = generate_external_id(
            external_id=_EXTERNAL_ID,
            state_code=_US_ND,
            id_type=_ID_TYPE)
        db_supervision_period = generate_supervision_period(
            person=db_person,
            external_id=_EXTERNAL_ID,
            start_date=_DATE_1,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO.value,
            state_code=_US_ND,
            supervising_officer=db_supervising_officer)
        db_supervision_period_another = generate_supervision_period(
            person=db_person,
            external_id=_EXTERNAL_ID_2,
            start_date=_DATE_2,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO.value,
            state_code=_US_ND,
            supervising_officer=db_supervising_officer)
        db_closed_supervision_period = generate_supervision_period(
            person=db_person,
            external_id=_EXTERNAL_ID_3,
            start_date=_DATE_3,
            termination_date=_DATE_4,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO.value,
            state_code=_US_ND,
            supervising_officer=db_supervising_officer)
        db_supervision_sentence = generate_supervision_sentence(
            person=db_person,
            external_id=_EXTERNAL_ID,
            supervision_periods=[db_supervision_period,
                                 db_supervision_period_another,
                                 db_closed_supervision_period])
        db_sentence_group = generate_sentence_group(
            external_id=_EXTERNAL_ID,
            supervision_sentences=[db_supervision_sentence])
        db_person.external_ids = [db_external_id]
        db_person.sentence_groups = [db_sentence_group]
        self._commit_to_db(db_person)

        external_id = attr.evolve(self.to_entity(db_external_id),
                                  person_external_id_id=None)
        new_supervising_officer = StateAgent.new_with_defaults(
            external_id=_EXTERNAL_ID_2,
            state_code=_US_ND,
            agent_type=StateAgentType.SUPERVISION_OFFICER)
        person = StatePerson.new_with_defaults(
            external_ids=[external_id],
            supervising_officer=new_supervising_officer,
            state_code=_US_ND)

        expected_supervising_officer = attr.evolve(
            self.to_entity(db_supervising_officer),
            agent_id=None)

        expected_new_supervising_officer = attr.evolve(new_supervising_officer)
        expected_supervision_period = attr.evolve(
            self.to_entity(db_supervision_period),
            supervising_officer=expected_new_supervising_officer)
        expected_supervision_period_another = attr.evolve(
            self.to_entity(db_supervision_period_another),
            supervising_officer=expected_new_supervising_officer)
        expected_closed_supervision_period = attr.evolve(
            self.to_entity(db_closed_supervision_period),
            supervising_officer=expected_supervising_officer)
        expected_supervision_sentence = attr.evolve(
            self.to_entity(db_supervision_sentence),
            supervision_periods=[expected_supervision_period,
                                 expected_supervision_period_another,
                                 expected_closed_supervision_period])
        expected_sentence_group = attr.evolve(
            self.to_entity(db_sentence_group),
            supervision_sentences=[expected_supervision_sentence])
        expected_external_id = self.to_entity(db_external_id)
        expected_person = attr.evolve(
            self.to_entity(db_person),
            external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group],
            supervising_officer=expected_new_supervising_officer)

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _US_ND, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session)
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_mergeIngestedAndDbSupervisionCaseTypeEntries(self) -> None:
        db_person = generate_person(state_code=_US_ND)
        db_external_id = generate_external_id(
            external_id=_EXTERNAL_ID,
            state_code=_US_ND,
            id_type=_ID_TYPE)
        db_supervision_case_type_entry = generate_supervision_case_type_entry(
            person=db_person,
            case_type=StateSupervisionCaseType.GENERAL.value,
            state_code=_US_ND,
            external_id=_EXTERNAL_ID)
        db_supervision_period = generate_supervision_period(
            person=db_person,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO.value,
            state_code=_US_ND,
            case_type_entries=[db_supervision_case_type_entry])
        db_supervision_sentence = generate_supervision_sentence(
            person=db_person,
            supervision_periods=[db_supervision_period])
        db_sentence_group = generate_sentence_group(person=db_person, supervision_sentences=[db_supervision_sentence])
        db_person.external_ids = [db_external_id]
        db_person.sentence_groups = [db_sentence_group]
        self._commit_to_db(db_person)

        external_id = attr.evolve(self.to_entity(db_external_id), person_external_id_id=None)
        person = StatePerson.new_with_defaults(external_ids=[external_id], state_code=_US_ND)
        new_case_type = StateSupervisionCaseTypeEntry.new_with_defaults(
            state_code=_US_ND,
            case_type=StateSupervisionCaseType.SEX_OFFENSE,
            external_id=_EXTERNAL_ID
        )
        supervision_period = attr.evolve(self.to_entity(db_supervision_period),
                                         case_type_entries=[new_case_type],
                                         supervision_period_id=None)
        supervision_sentence = attr.evolve(self.to_entity(db_supervision_sentence),
                                           supervision_periods=[supervision_period],
                                           supervision_sentence_id=None)
        sentence_group = attr.evolve(self.to_entity(db_sentence_group),
                                     supervision_sentences=[supervision_sentence],
                                     sentence_group_id=None)
        person.sentence_groups = [sentence_group]

        expected_supervision_case_type = attr.evolve(new_case_type)
        expected_supervision_period = attr.evolve(self.to_entity(db_supervision_period),
                                                  case_type_entries=[expected_supervision_case_type])
        expected_supervision_sentence = attr.evolve(self.to_entity(db_supervision_sentence),
                                                    supervision_periods=[expected_supervision_period])
        expected_sentence_group = attr.evolve(self.to_entity(db_sentence_group),
                                              supervision_sentences=[expected_supervision_sentence])
        expected_external_id = self.to_entity(db_external_id)
        expected_person = attr.evolve(self.to_entity(db_person),
                                      external_ids=[expected_external_id],
                                      sentence_groups=[expected_sentence_group])

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(session, _US_ND, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit([expected_person], matched_entities.people, session)
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)
