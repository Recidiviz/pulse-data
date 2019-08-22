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
"""Tests for state_matching_utils.py"""
import datetime
from unittest import TestCase

import attr
import pytest

from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodStatus, StateIncarcerationPeriodAdmissionReason, \
    StateIncarcerationPeriodReleaseReason
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_violation_response \
    import StateSupervisionViolationResponseRevocationType
from recidiviz.persistence.entity.state.entities import StatePersonExternalId, \
    StatePerson, StatePersonAlias, StateCharge, StateSentenceGroup, StateFine, \
    StateIncarcerationPeriod, StateIncarcerationIncident, \
    StateIncarcerationSentence, StateCourtCase, StateSupervisionSentence, \
    StateSupervisionViolationResponse, StateSupervisionViolation, \
    StateSupervisionPeriod
from recidiviz.persistence.entity_matching.state.state_matching_utils import \
    _is_match, generate_child_entity_trees, add_child_to_entity, \
    remove_child_from_entity, _merge_incarceration_periods_helper, \
    move_incidents_onto_periods, merge_flat_fields, \
    get_root_entity_cls, get_total_entities_of_cls, \
    associate_revocation_svrs_with_ips, admitted_for_revocation, \
    revoked_to_prison, base_entity_match
from recidiviz.persistence.entity_matching.entity_matching_types import \
    EntityTree
from recidiviz.persistence.errors import EntityMatchingError

_DATE_1 = datetime.date(year=2019, month=1, day=1)
_DATE_2 = datetime.date(year=2019, month=2, day=1)
_DATE_3 = datetime.date(year=2019, month=3, day=1)
_DATE_4 = datetime.date(year=2019, month=4, day=1)
_DATE_5 = datetime.date(year=2019, month=5, day=1)
_DATE_6 = datetime.date(year=2019, month=6, day=1)
_DATE_7 = datetime.date(year=2019, month=5, day=1)
_EXTERNAL_ID = 'EXTERNAL_ID-1'
_EXTERNAL_ID_2 = 'EXTERNAL_ID-2'
_EXTERNAL_ID_3 = 'EXTERNAL_ID-3'
_EXTERNAL_ID_4 = 'EXTERNAL_ID-4'
_EXTERNAL_ID_5 = 'EXTERNAL_ID-5'
_EXTERNAL_ID_6 = 'EXTERNAL_ID-6'
_ID = 1
_ID_2 = 2
_ID_3 = 3
_COUNTY_CODE = 'COUNTY'
_STATE_CODE = 'NC'
_STATE_CODE_ANOTHER = 'CA'
_FULL_NAME = 'NAME'
_ID_TYPE = 'ID_TYPE'
_ID_TYPE_ANOTHER = 'ID_TYPE_ANOTHER'
_FACILITY = 'FACILITY'
_FACILITY_2 = 'FACILITY_2'
_FACILITY_3 = 'FACILITY_3'
_FACILITY_4 = 'FACILITY_4'


# pylint: disable=protected-access
class TestStateMatchingUtils(TestCase):
    """Tests for state entity matching utils"""

    def test_isMatch_statePerson(self):
        external_id = StatePersonExternalId.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID)
        external_id_different = attr.evolve(
            external_id, external_id=_EXTERNAL_ID_2)
        person = StatePerson.new_with_defaults(
            full_name='name', external_ids=[external_id])
        person_another = StatePerson.new_with_defaults(
            full_name='name_2', external_ids=[external_id])

        self.assertTrue(
            _is_match(ingested_entity=person, db_entity=person_another))
        person_another.external_ids = [external_id_different]
        self.assertFalse(
            _is_match(ingested_entity=person, db_entity=person_another))

    def test_isMatch_statePersonExternalId_type(self):
        external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        external_id_different = attr.evolve(
            external_id, person_external_id_id=None)
        self.assertTrue(_is_match(ingested_entity=external_id,
                                  db_entity=external_id_different))

        external_id.id_type = _ID_TYPE_ANOTHER
        self.assertFalse(_is_match(ingested_entity=external_id,
                                   db_entity=external_id_different))

    def test_isMatch_statePersonExternalId_externalId(self):
        external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        external_id_different = attr.evolve(
            external_id, person_external_id_id=None)
        self.assertTrue(_is_match(ingested_entity=external_id,
                                  db_entity=external_id_different))

        external_id.external_id = _EXTERNAL_ID_2
        self.assertFalse(_is_match(ingested_entity=external_id,
                                   db_entity=external_id_different))

    def test_isMatch_statePersonExternalId_stateCode(self):
        external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        external_id_different = attr.evolve(
            external_id, person_external_id_id=None)
        self.assertTrue(_is_match(ingested_entity=external_id,
                                  db_entity=external_id_different))

        external_id.state_code = _STATE_CODE_ANOTHER
        self.assertFalse(_is_match(ingested_entity=external_id,
                                   db_entity=external_id_different))

    def test_isMatch_statePersonAlias(self):
        alias = StatePersonAlias.new_with_defaults(
            state_code=_STATE_CODE, full_name='full_name')
        alias_another = attr.evolve(alias)
        self.assertTrue(
            _is_match(ingested_entity=alias, db_entity=alias_another))
        alias_another.state_code = _STATE_CODE_ANOTHER
        self.assertFalse(
            _is_match(ingested_entity=alias, db_entity=alias_another))

    def test_isMatch_defaultCompareExternalId(self):
        charge = StateCharge.new_with_defaults(
            external_id=_EXTERNAL_ID, description='description')
        charge_another = attr.evolve(charge, description='description_another')
        self.assertTrue(
            _is_match(ingested_entity=charge, db_entity=charge_another))
        charge.external_id = _EXTERNAL_ID_2
        self.assertFalse(
            _is_match(ingested_entity=charge, db_entity=charge_another))

    def test_isMatch_defaultCompareNoExternalIds(self):
        charge = StateCharge.new_with_defaults()
        charge_another = attr.evolve(charge)
        self.assertTrue(
            _is_match(ingested_entity=charge, db_entity=charge_another))
        charge.description = 'description'
        self.assertFalse(
            _is_match(ingested_entity=charge, db_entity=charge_another))

    def test_mergeFlatFields(self):
        ing_entity = StateSentenceGroup.new_with_defaults(
            county_code='county_code-updated', max_length_days=10,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO)
        db_entity = StateSentenceGroup.new_with_defaults(
            sentence_group_id=_ID, county_code='county_code',
            status=StateSentenceStatus.SERVING)
        expected_entity = attr.evolve(ing_entity, sentence_group_id=_ID,
                                      status=StateSentenceStatus.SERVING)

        self.assertEqual(
            expected_entity,
            merge_flat_fields(new_entity=ing_entity, old_entity=db_entity))

    def test_mergeFlatFields_incompleteIncarcerationPeriods(self):
        ingested_entity = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=_ID,
            external_id=_EXTERNAL_ID,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            facility=_FACILITY, admission_date=_DATE_1,
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION)
        db_entity = StateIncarcerationPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID_2,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            facility=_FACILITY, release_date=_DATE_2,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)
        expected_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=_ID,
                external_id=_EXTERNAL_ID + '|' + _EXTERNAL_ID_2,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                facility=_FACILITY, admission_date=_DATE_1,
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                release_date=_DATE_2,
                release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)
        self.assertEqual(
            expected_incarceration_period,
            merge_flat_fields(new_entity=ingested_entity, old_entity=db_entity))

    def test_generateChildEntitiesWithAncestorChain(self):
        fine = StateFine.new_with_defaults(fine_id=_ID)
        fine_another = StateFine.new_with_defaults(fine_id=_ID_2)
        person = StatePerson.new_with_defaults(person_id=_ID)
        sentence_group = StateSentenceGroup.new_with_defaults(
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            state_code=_STATE_CODE,
            fines=[fine, fine_another],
            person=[person], sentence_group_id=_ID)
        sentence_group_tree = EntityTree(
            entity=sentence_group, ancestor_chain=[person])

        expected_child_trees = [
            EntityTree(
                entity=fine, ancestor_chain=[person, sentence_group]),
            EntityTree(
                entity=fine_another, ancestor_chain=[person, sentence_group]),
        ]

        self.assertEqual(
            expected_child_trees,
            generate_child_entity_trees(
                'fines', [sentence_group_tree]))

    def test_addChildToEntity(self):
        fine = StateFine.new_with_defaults(fine_id=_ID)
        sentence_group = StateSentenceGroup.new_with_defaults(
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            state_code=_STATE_CODE,
            fines=[], sentence_group_id=_ID)

        expected_sentence_group = attr.evolve(
            sentence_group, fines=[fine])
        add_child_to_entity(entity=sentence_group,
                            child_field_name='fines',
                            child_to_add=fine)
        self.assertEqual(expected_sentence_group, sentence_group)

    def test_addChildToEntity_singular(self):
        charge = StateCharge.new_with_defaults(charge_id=_ID)
        court_case = StateCourtCase.new_with_defaults(court_case_id=_ID)

        expected_charge = attr.evolve(charge, court_case=court_case)
        add_child_to_entity(entity=charge,
                            child_field_name='court_case',
                            child_to_add=court_case)
        self.assertEqual(expected_charge, charge)

    def test_removeChildFromEntity(self):
        fine = StateFine.new_with_defaults(fine_id=_ID)
        fine_another = StateFine.new_with_defaults(fine_id=_ID_2)
        sentence_group = StateSentenceGroup.new_with_defaults(
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            state_code=_STATE_CODE,
            fines=[fine, fine_another],
            sentence_group_id=_ID)

        expected_sentence_group = attr.evolve(sentence_group, fines=[fine])
        remove_child_from_entity(
            entity=sentence_group, child_field_name='fines',
            child_to_remove=fine_another)
        self.assertEqual(expected_sentence_group, sentence_group)

    def test_getRootEntity(self):
        incarceration_incident = StateIncarcerationIncident.new_with_defaults(
            external_id=_EXTERNAL_ID)
        placeholder_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_incidents=[incarceration_incident])
        placeholder_incarceration_sentence = \
            StateIncarcerationSentence.new_with_defaults(
                incarceration_periods=[placeholder_incarceration_period])
        placeholder_sentence_group = StateSentenceGroup.new_with_defaults(
            sentence_group_id=None,
            incarceration_sentences=[placeholder_incarceration_sentence])
        person = StatePerson.new_with_defaults(
            sentence_groups=[placeholder_sentence_group])
        self.assertEqual(StateIncarcerationIncident,
                         get_root_entity_cls([person]))

    def test_getRootEntity_emptyList_raises(self):
        with pytest.raises(EntityMatchingError):
            get_root_entity_cls([])

    def test_getRootEntity_allPlaceholders_raises(self):
        placeholder_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults()
        placeholder_incarceration_sentence = \
            StateIncarcerationSentence.new_with_defaults(
                incarceration_periods=[placeholder_incarceration_period])
        placeholder_sentence_group = StateSentenceGroup.new_with_defaults(
            incarceration_sentences=[placeholder_incarceration_sentence])
        placeholder_person = StatePerson.new_with_defaults(
            sentence_groups=[placeholder_sentence_group])
        with pytest.raises(EntityMatchingError):
            get_root_entity_cls([placeholder_person])

    def test_getTotalEntitiesOfCls(self):
        supervision_sentence = StateSupervisionSentence.new_with_defaults()
        supervision_sentence_2 = attr.evolve(supervision_sentence)
        supervision_sentence_3 = attr.evolve(supervision_sentence)
        sentence_group = StateSentenceGroup.new_with_defaults(
            supervision_sentences=[supervision_sentence,
                                   supervision_sentence_2])
        sentence_group_2 = StateSentenceGroup.new_with_defaults(
            supervision_sentences=[supervision_sentence_2,
                                   supervision_sentence_3])
        person = StatePerson.new_with_defaults(
            sentence_groups=[sentence_group, sentence_group_2])

        self.assertEqual(3, get_total_entities_of_cls(
            [person], StateSupervisionSentence))
        self.assertEqual(2, get_total_entities_of_cls(
            [person], StateSentenceGroup))
        self.assertEqual(1, get_total_entities_of_cls([person], StatePerson))

    def test_completeEnumSet_AdmittedForRevication(self):
        period = StateIncarcerationPeriod.new_with_defaults()
        for admission_reason in StateIncarcerationPeriodAdmissionReason:
            period.admission_reason = admission_reason
            admitted_for_revocation(period)

    def test_completeEnumSet_RevokedToPrison(self):
        svr = StateSupervisionViolationResponse.new_with_defaults()
        for revocation_type in StateSupervisionViolationResponseRevocationType:
            svr.revocation_type = revocation_type
            revoked_to_prison(svr)

    def test_associateSvrsWithIps(self):
        # Arrange
        svr_1 = StateSupervisionViolationResponse.new_with_defaults(
            response_date=_DATE_1,
            revocation_type=StateSupervisionViolationResponseRevocationType.
            REINCARCERATION)
        svr_2 = StateSupervisionViolationResponse.new_with_defaults(
            response_date=_DATE_3,
            revocation_type=StateSupervisionViolationResponseRevocationType.
            REINCARCERATION)
        svr_3 = StateSupervisionViolationResponse.new_with_defaults(
            response_date=_DATE_5,
            revocation_type=StateSupervisionViolationResponseRevocationType.
            RETURN_TO_SUPERVISION)
        placeholder_sv = StateSupervisionViolation.new_with_defaults(
            supervision_violation_responses=[svr_1, svr_2, svr_3])
        placeholder_sp = StateSupervisionPeriod.new_with_defaults(
            supervision_violations=[placeholder_sv])
        placeholder_ss = StateSupervisionSentence.new_with_defaults(
            supervision_periods=[placeholder_sp])

        ip_1 = StateIncarcerationPeriod.new_with_defaults(
            admission_date=_DATE_2,
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION)
        ip_2 = StateIncarcerationPeriod.new_with_defaults(
            admission_date=_DATE_4,
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION)
        placeholder_is = StateIncarcerationSentence.new_with_defaults(
            incarceration_periods=[ip_1, ip_2])
        placeholder_sg = StateSentenceGroup.new_with_defaults(
            incarceration_sentences=[placeholder_is],
            supervision_sentences=[placeholder_ss])
        placeholder_person = StatePerson.new_with_defaults(
            sentence_groups=[placeholder_sg])

        external_id = StatePersonExternalId.new_with_defaults(
            external_id=_EXTERNAL_ID)
        person_without_revocation = StatePerson.new_with_defaults(
            external_ids=[external_id])

        expected_svr_1 = attr.evolve(svr_1)
        expected_svr_2 = attr.evolve(svr_2)
        expected_svr_3 = attr.evolve(svr_3)
        expected_placeholder_sv = attr.evolve(
            placeholder_sv,
            supervision_violation_responses=[expected_svr_1, expected_svr_2,
                                             expected_svr_3])
        expected_placeholder_sp = attr.evolve(
            placeholder_sp, supervision_violations=[expected_placeholder_sv])
        expected_placeholder_ss = attr.evolve(
            placeholder_ss, supervision_periods=[expected_placeholder_sp])

        expected_ip_1 = attr.evolve(ip_1,
                                    source_supervision_violation_response=svr_1)
        expected_ip_2 = attr.evolve(ip_2,
                                    source_supervision_violation_response=svr_2)

        expected_placeholder_is = attr.evolve(
            placeholder_is,
            incarceration_periods=[expected_ip_1, expected_ip_2])
        expected_placeholder_sg = attr.evolve(
            placeholder_sg,
            supervision_sentences=[expected_placeholder_ss],
            incarceration_sentences=[expected_placeholder_is])
        expected_placeholder_person = attr.evolve(
            placeholder_person, sentence_groups=[expected_placeholder_sg])
        expected_person_without_revocation = attr.evolve(
            person_without_revocation)

        # Act
        associate_revocation_svrs_with_ips(
            [person_without_revocation, placeholder_person])

        # Assert
        self.assertEqual(expected_person_without_revocation,
                         person_without_revocation)
        self.assertEqual(expected_placeholder_person, placeholder_person)

    def test_associateSvrsWithIps_onlyRevocationTypes(self):
        # Arrange
        svr_1 = StateSupervisionViolationResponse.new_with_defaults(
            response_date=_DATE_1,
            revocation_type=StateSupervisionViolationResponseRevocationType.
            REINCARCERATION)
        svr_2 = StateSupervisionViolationResponse.new_with_defaults(
            response_date=_DATE_3,
            revocation_type=StateSupervisionViolationResponseRevocationType.
            RETURN_TO_SUPERVISION)
        placeholder_sv = StateSupervisionViolation.new_with_defaults(
            supervision_violation_responses=[svr_1, svr_2])
        placeholder_sp = StateSupervisionPeriod.new_with_defaults(
            supervision_violations=[placeholder_sv])
        placeholder_ss = StateSupervisionSentence.new_with_defaults(
            supervision_periods=[placeholder_sp])

        ip_1 = StateIncarcerationPeriod.new_with_defaults(
            admission_date=_DATE_2,
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION)
        ip_2 = StateIncarcerationPeriod.new_with_defaults(
            admission_date=_DATE_4,
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION)
        placeholder_is = StateIncarcerationSentence.new_with_defaults(
            incarceration_periods=[ip_1, ip_2])
        placeholder_sg = StateSentenceGroup.new_with_defaults(
            incarceration_sentences=[placeholder_is],
            supervision_sentences=[placeholder_ss])
        placeholder_person = StatePerson.new_with_defaults(
            sentence_groups=[placeholder_sg])

        expected_svr_1 = attr.evolve(svr_1)
        expected_svr_2 = attr.evolve(svr_2)
        expected_placeholder_sv = attr.evolve(
            placeholder_sv,
            supervision_violation_responses=[expected_svr_1, expected_svr_2])
        expected_placeholder_sp = attr.evolve(
            placeholder_sp, supervision_violations=[expected_placeholder_sv])
        expected_placeholder_ss = attr.evolve(
            placeholder_ss, supervision_periods=[expected_placeholder_sp])

        expected_ip_1 = attr.evolve(ip_1)
        expected_ip_2 = attr.evolve(
            ip_2, source_supervision_violation_response=svr_1)

        expected_placeholder_is = attr.evolve(
            placeholder_is,
            incarceration_periods=[expected_ip_1, expected_ip_2])
        expected_placeholder_sg = attr.evolve(
            placeholder_sg,
            supervision_sentences=[expected_placeholder_ss],
            incarceration_sentences=[expected_placeholder_is])
        expected_placeholder_person = attr.evolve(
            placeholder_person, sentence_groups=[expected_placeholder_sg])

        # Act
        associate_revocation_svrs_with_ips([placeholder_person])

        # Assert
        self.assertEqual(expected_placeholder_person, placeholder_person)

    def test_associateSvrsWithIps_dontAssociateTheSameSvr(self):
        # Arrange
        svr_1 = StateSupervisionViolationResponse.new_with_defaults(
            response_date=_DATE_1,
            revocation_type=StateSupervisionViolationResponseRevocationType.
            REINCARCERATION)
        placeholder_sv = StateSupervisionViolation.new_with_defaults(
            supervision_violation_responses=[svr_1])
        placeholder_sp = StateSupervisionPeriod.new_with_defaults(
            supervision_violations=[placeholder_sv])
        placeholder_ss = StateSupervisionSentence.new_with_defaults(
            supervision_periods=[placeholder_sp])

        ip_1 = StateIncarcerationPeriod.new_with_defaults(
            admission_date=_DATE_2,
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION)
        ip_2 = StateIncarcerationPeriod.new_with_defaults(
            admission_date=_DATE_4,
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION)
        placeholder_is = StateIncarcerationSentence.new_with_defaults(
            incarceration_periods=[ip_1, ip_2])
        placeholder_sg = StateSentenceGroup.new_with_defaults(
            incarceration_sentences=[placeholder_is],
            supervision_sentences=[placeholder_ss])
        placeholder_person = StatePerson.new_with_defaults(
            sentence_groups=[placeholder_sg])

        expected_svr_1 = attr.evolve(svr_1)
        expected_placeholder_sv = attr.evolve(
            placeholder_sv,
            supervision_violation_responses=[expected_svr_1])
        expected_placeholder_sp = attr.evolve(
            placeholder_sp, supervision_violations=[expected_placeholder_sv])
        expected_placeholder_ss = attr.evolve(
            placeholder_ss, supervision_periods=[expected_placeholder_sp])

        expected_ip_1 = attr.evolve(
            ip_1, source_supervision_violation_response=svr_1)
        expected_ip_2 = attr.evolve(ip_2)

        expected_placeholder_is = attr.evolve(
            placeholder_is,
            incarceration_periods=[expected_ip_1, expected_ip_2])
        expected_placeholder_sg = attr.evolve(
            placeholder_sg,
            supervision_sentences=[expected_placeholder_ss],
            incarceration_sentences=[expected_placeholder_is])
        expected_placeholder_person = attr.evolve(
            placeholder_person, sentence_groups=[expected_placeholder_sg])

        # Act
        associate_revocation_svrs_with_ips([placeholder_person])

        # Assert
        self.assertEqual(expected_placeholder_person, placeholder_person)

    def test_mergeIncarcerationPeriods(self):
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            facility=_FACILITY, admission_date=_DATE_1,
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION)
        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID_2,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            facility=_FACILITY, release_date=_DATE_2,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)
        incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID_3,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            facility=_FACILITY_2, admission_date=_DATE_2,
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.TRANSFER)
        incarceration_period_4 = StateIncarcerationPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID_4,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            facility=_FACILITY_2, release_date=_DATE_3,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)
        incarceration_period_5 = StateIncarcerationPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID_5,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            facility=_FACILITY, admission_date=_DATE_4,
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.TRANSFER)
        incarceration_period_6 = StateIncarcerationPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID_6,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            facility=_FACILITY_3, release_date=_DATE_5,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)
        placeholder_period = StateIncarcerationPeriod.new_with_defaults()

        expected_merged_incarceration_period_1 = \
            StateIncarcerationPeriod.new_with_defaults(
                external_id=_EXTERNAL_ID + '|' + _EXTERNAL_ID_2,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                facility=_FACILITY, admission_date=_DATE_1,
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                release_date=_DATE_2,
                release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)
        expected_merged_incarceration_period_2 = \
            StateIncarcerationPeriod.new_with_defaults(
                external_id=_EXTERNAL_ID_3 + '|' + _EXTERNAL_ID_4,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                facility=_FACILITY_2, admission_date=_DATE_2,
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.TRANSFER,
                release_date=_DATE_3,
                release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)
        expected_unmerged_incarceration_period = attr.evolve(
            incarceration_period_5)
        expected_unmerged_incarceration_period_another = attr.evolve(
            incarceration_period_6)
        expected_placeholder_period = attr.evolve(placeholder_period)

        expected_incarceration_periods = [
            expected_placeholder_period,
            expected_merged_incarceration_period_1,
            expected_merged_incarceration_period_2,
            expected_unmerged_incarceration_period,
            expected_unmerged_incarceration_period_another]

        ingested_incarceration_periods = [
            placeholder_period, incarceration_period_1, incarceration_period_5,
            incarceration_period_2, incarceration_period_4,
            incarceration_period_3, incarceration_period_6
        ]

        merged_periods = _merge_incarceration_periods_helper(
            ingested_incarceration_periods)

        self.assertCountEqual(expected_incarceration_periods, merged_periods)

    def test_mergeIncarcerationPeriods_multipleTransfersSameDate(self):
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            facility=_FACILITY, admission_date=_DATE_1,
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION)
        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID_2,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            facility=_FACILITY, release_date=_DATE_2,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)
        incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID_3,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            facility=_FACILITY_2, admission_date=_DATE_2,
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.TRANSFER)
        incarceration_period_4 = StateIncarcerationPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID_4,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            facility=_FACILITY_2, release_date=_DATE_2,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)
        incarceration_period_5 = StateIncarcerationPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID_5,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            facility=_FACILITY_3, admission_date=_DATE_2,
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.TRANSFER)
        incarceration_period_6 = StateIncarcerationPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID_6,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            facility=_FACILITY_3, release_date=_DATE_2,
            release_reason=
            StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)

        expected_merged_incarceration_period_1 = \
            StateIncarcerationPeriod.new_with_defaults(
                external_id=_EXTERNAL_ID + '|' + _EXTERNAL_ID_2,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                facility=_FACILITY, admission_date=_DATE_1,
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                release_date=_DATE_2,
                release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)
        expected_merged_incarceration_period_2 = \
            StateIncarcerationPeriod.new_with_defaults(
                external_id=_EXTERNAL_ID_3 + '|' + _EXTERNAL_ID_4,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                facility=_FACILITY_2, admission_date=_DATE_2,
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.TRANSFER,
                release_date=_DATE_2,
                release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)
        expected_merged_incarceration_period_3 = \
            StateIncarcerationPeriod.new_with_defaults(
                external_id=_EXTERNAL_ID_5 + '|' + _EXTERNAL_ID_6,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                facility=_FACILITY_3, admission_date=_DATE_2,
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.TRANSFER,
                release_date=_DATE_2,
                release_reason=
                StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)

        expected_incarceration_periods = [
            expected_merged_incarceration_period_1,
            expected_merged_incarceration_period_2,
            expected_merged_incarceration_period_3]

        ingested_incarceration_periods = [
            incarceration_period_1, incarceration_period_5,
            incarceration_period_2, incarceration_period_4,
            incarceration_period_3, incarceration_period_6
        ]

        merged_periods = _merge_incarceration_periods_helper(
            ingested_incarceration_periods)

        self.assertCountEqual(expected_incarceration_periods, merged_periods)

    def test_mergeIncarcerationPeriods_doNotMergeNonConsecutiveSequences(self):
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            facility=_FACILITY, admission_date=_DATE_1,
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION)
        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID_3,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            facility=_FACILITY, release_date=_DATE_2,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)
        ingested_incarceration_periods = [
            incarceration_period_1, incarceration_period_2]

        expected_incarceration_periods = [
            attr.evolve(incarceration_period_1),
            attr.evolve(incarceration_period_2)]
        merged_periods = _merge_incarceration_periods_helper(
            ingested_incarceration_periods)
        self.assertCountEqual(expected_incarceration_periods, merged_periods)

        incarceration_period_2.external_id = _EXTERNAL_ID_2
        expected_merged_incarceration_period_1 = \
            StateIncarcerationPeriod.new_with_defaults(
                external_id=_EXTERNAL_ID + '|' + _EXTERNAL_ID_2,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                facility=_FACILITY, admission_date=_DATE_1,
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                release_date=_DATE_2,
                release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)
        expected_incarceration_periods = [
            expected_merged_incarceration_period_1]
        merged_periods = _merge_incarceration_periods_helper(
            ingested_incarceration_periods)
        self.assertCountEqual(expected_incarceration_periods, merged_periods)

    def test_mergeIncarcerationPeriods_doNotMergeWithPlaceholder(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            facility=_FACILITY, admission_date=_DATE_1,
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION)
        placeholder_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults()
        ingested_incarceration_periods = [
            incarceration_period, placeholder_incarceration_period]

        expected_periods = [
            attr.evolve(incarceration_period),
            attr.evolve(placeholder_incarceration_period)]
        merged_periods = _merge_incarceration_periods_helper(
            ingested_incarceration_periods)
        self.assertCountEqual(expected_periods, merged_periods)

    def test_moveIncidentsOntoPeriods(self):
        merged_incarceration_period_1 = \
            StateIncarcerationPeriod.new_with_defaults(
                external_id=_EXTERNAL_ID + '|' + _EXTERNAL_ID_2,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                facility=_FACILITY, admission_date=_DATE_1,
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                release_date=_DATE_3,
                release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)
        merged_incarceration_period_2 = \
            StateIncarcerationPeriod.new_with_defaults(
                external_id=_EXTERNAL_ID_3 + '|' + _EXTERNAL_ID_4,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                facility=_FACILITY_2, admission_date=_DATE_3,
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.TRANSFER,
                release_date=_DATE_5,
                release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)
        unmerged_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                external_id=_EXTERNAL_ID_5,
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                facility=_FACILITY_3, admission_date=_DATE_5,
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.TRANSFER)

        incident_1 = StateIncarcerationIncident.new_with_defaults(
            external_id=_EXTERNAL_ID, facility=_FACILITY, incident_date=_DATE_2)
        incident_2 = StateIncarcerationIncident.new_with_defaults(
            external_id=_EXTERNAL_ID_2, facility=_FACILITY_2,
            incident_date=_DATE_4)
        incident_3 = StateIncarcerationIncident.new_with_defaults(
            external_id=_EXTERNAL_ID_3, facility=_FACILITY_4,
            incident_date=_DATE_7)
        placeholder_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_incidents=[incident_1, incident_2, incident_3])

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID,
            incarceration_periods=[
                merged_incarceration_period_1,
                merged_incarceration_period_2,
                unmerged_incarceration_period])
        placeholder_incarceration_sentence = \
            StateIncarcerationSentence.new_with_defaults(
                external_id=_EXTERNAL_ID_2,
                incarceration_periods=[placeholder_incarceration_period])
        sentence_group = StateSentenceGroup.new_with_defaults(
            incarceration_sentences=[incarceration_sentence,
                                     placeholder_incarceration_sentence])

        person = StatePerson.new_with_defaults(sentence_groups=[sentence_group])

        expected_merged_period = attr.evolve(
            merged_incarceration_period_1, incarceration_incidents=[incident_1])
        expected_merged_period_2 = attr.evolve(
            merged_incarceration_period_2, incarceration_incidents=[incident_2])
        expected_unmerged_period = attr.evolve(unmerged_incarceration_period)
        expected_placeholder_period = attr.evolve(
            placeholder_incarceration_period,
            incarceration_incidents=[incident_3])
        expected_sentence = attr.evolve(
            incarceration_sentence,
            incarceration_periods=[
                expected_merged_period,
                expected_merged_period_2,
                expected_unmerged_period])
        expected_placeholder_sentence = attr.evolve(
            placeholder_incarceration_sentence,
            incarceration_periods=[expected_placeholder_period])
        expected_sentence_group = attr.evolve(
            sentence_group, incarceration_sentences=[
                expected_sentence, expected_placeholder_sentence])
        expected_person = attr.evolve(
            person, sentence_groups=[expected_sentence_group])

        move_incidents_onto_periods([person])
        self.assertEqual(expected_person, person)

    def test_baseEntityMatch_placeholder(self):
        charge = StateCharge.new_with_defaults()
        charge_another = StateCharge.new_with_defaults()
        self.assertFalse(
            base_entity_match(
                ingested_entity=EntityTree(entity=charge, ancestor_chain=[]),
                db_entity=EntityTree(entity=charge_another, ancestor_chain=[])))

    def test_baseEntityMatch_externalIdCompare(self):
        charge = StateCharge.new_with_defaults(external_id=_EXTERNAL_ID)
        charge_another = StateCharge.new_with_defaults()
        self.assertFalse(
            base_entity_match(
                ingested_entity=EntityTree(entity=charge, ancestor_chain=[]),
                db_entity=EntityTree(entity=charge_another, ancestor_chain=[])))
        charge_another.external_id = _EXTERNAL_ID
        self.assertTrue(
            base_entity_match(
                ingested_entity=EntityTree(entity=charge, ancestor_chain=[]),
                db_entity=EntityTree(entity=charge_another, ancestor_chain=[])))

    def test_baseEntityMatch_flatFieldsCompare(self):
        charge = StateCharge.new_with_defaults(
            state_code=_STATE_CODE, county_code=_COUNTY_CODE)
        charge_another = StateCharge.new_with_defaults(state_code=_STATE_CODE)
        self.assertFalse(
            base_entity_match(
                ingested_entity=EntityTree(entity=charge, ancestor_chain=[]),
                db_entity=EntityTree(entity=charge_another, ancestor_chain=[])))
        charge_another.county_code = _COUNTY_CODE
        self.assertTrue(
            base_entity_match(
                ingested_entity=EntityTree(entity=charge, ancestor_chain=[]),
                db_entity=EntityTree(entity=charge_another, ancestor_chain=[])))
