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
"""Tests for us_nd_state_matching_utils.py"""
import datetime
from typing import List
from unittest import TestCase

import attr
from mock import create_autospec

from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.common.constants.state.state_incarceration import \
    StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodStatus, StateIncarcerationPeriodAdmissionReason, \
    StateIncarcerationPeriodReleaseReason
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_violation_response \
    import StateSupervisionViolationResponseRevocationType
from recidiviz.persistence.database.base_schema import StateBase
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.database.schema_entity_converter import \
    schema_entity_converter as converter
from recidiviz.persistence.entity.state.entities import StatePersonExternalId, \
    StatePerson, StateSentenceGroup, \
    StateIncarcerationPeriod, StateIncarcerationIncident, \
    StateIncarcerationSentence, StateSupervisionSentence, \
    StateSupervisionViolationResponse, StateSupervisionViolation, \
    StateSupervisionPeriod
from recidiviz.persistence.entity_matching.state.state_matching_utils import \
     default_merge_flat_fields
from recidiviz.persistence.entity_matching.state.us_nd.\
    us_nd_matching_utils import \
    merge_incomplete_periods, _update_temporary_holds_helper, \
    associate_revocation_svrs_with_ips, _merge_incarceration_periods_helper, \
    move_incidents_onto_periods
from recidiviz.tests.utils import fakes
from recidiviz.utils.regions import Region

_DATE_1 = datetime.date(year=2019, month=1, day=1)
_DATE_2 = datetime.date(year=2019, month=2, day=1)
_DATE_3 = datetime.date(year=2019, month=3, day=1)
_DATE_4 = datetime.date(year=2019, month=4, day=1)
_DATE_5 = datetime.date(year=2019, month=5, day=1)
_DATE_6 = datetime.date(year=2019, month=6, day=1)
_DATE_7 = datetime.date(year=2019, month=7, day=1)
_DATE_8 = datetime.date(year=2019, month=8, day=1)
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
class TestUsNdMatchingUtils(TestCase):
    """Test class for US_ND specific matching utils."""

    def setUp(self) -> None:
        fakes.use_in_memory_sqlite_database(StateBase)

    def to_entity(self, schema_obj):
        return converter.convert_schema_object_to_entity(
            schema_obj, populate_back_edges=False)

    def assert_schema_objects_equal(self,
                                    expected: StateBase,
                                    actual: StateBase):
        self.assertEqual(
            converter.convert_schema_object_to_entity(expected),
            converter.convert_schema_object_to_entity(actual)
        )

    def assert_schema_object_lists_equal(self,
                                         expected: List[StateBase],
                                         actual: List[StateBase]):
        self.assertCountEqual(
            converter.convert_schema_objects_to_entity(expected),
            converter.convert_schema_objects_to_entity(actual)
        )

    def assert_people_match(self,
                            expected_people: List[StatePerson],
                            matched_people: List[schema.StatePerson]):
        converted_matched = \
            converter.convert_schema_objects_to_entity(matched_people)
        db_expected_with_backedges = \
            converter.convert_entity_people_to_schema_people(expected_people)
        expected_with_backedges = \
            converter.convert_schema_objects_to_entity(
                db_expected_with_backedges)
        self.assertEqual(expected_with_backedges, converted_matched)

    def create_fake_nd_region(self):
        fake_region = create_autospec(Region)
        overrides_builder = EnumOverrides.Builder()
        overrides_builder.add(
            'PV', StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION)
        overrides_builder.add(
            'REC', StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE)
        overrides_builder.add(
            'ADM', StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION)
        fake_region.get_enum_overrides.return_value = overrides_builder.build()
        return fake_region

    def test_mergeFlatFields(self):
        ing_entity = schema.StateSentenceGroup(
            county_code='county_code-updated', max_length_days=10,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value)
        db_entity = schema.StateSentenceGroup(
            sentence_group_id=_ID, county_code='county_code',
            status=StateSentenceStatus.SERVING)
        expected_entity = schema.StateSentenceGroup(
            sentence_group_id=_ID,
            county_code='county_code-updated',
            max_length_days=10,
            status=StateSentenceStatus.SERVING.value)

        merged_entity = default_merge_flat_fields(
            new_entity=ing_entity, old_entity=db_entity)
        self.assert_schema_objects_equal(expected_entity, merged_entity)

    def test_mergeFlatFields_incompleteIncarcerationPeriods(self):
        ingested_entity = schema.StateIncarcerationPeriod(
            incarceration_period_id=_ID,
            external_id=_EXTERNAL_ID,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            facility=_FACILITY, admission_date=_DATE_1,
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION)
        db_entity = schema.StateIncarcerationPeriod(
            external_id=_EXTERNAL_ID_2,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            facility=_FACILITY, release_date=_DATE_2,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)
        expected_incarceration_period = \
            schema.StateIncarcerationPeriod(
                incarceration_period_id=_ID,
                external_id=_EXTERNAL_ID + '|' + _EXTERNAL_ID_2,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                facility=_FACILITY, admission_date=_DATE_1,
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                release_date=_DATE_2,
                release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)
        self.assert_schema_objects_equal(
            expected_incarceration_period,
            merge_incomplete_periods(new_entity=ingested_entity,
                                     old_entity=db_entity))

    def test_transformToHolds(self):
        # Arrange
        ip = schema.StateIncarcerationPeriod(
            admission_date=_DATE_1,
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.
            PAROLE_REVOCATION.value,
            admission_reason_raw_text='PV',
            release_date=_DATE_2,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER.value,
            incarceration_type=StateIncarcerationType.EXTERNAL_UNKNOWN.value
        )
        ip_2 = schema.StateIncarcerationPeriod(
            admission_date=_DATE_2,
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.TRANSFER.value,
            admission_reason_raw_text='INT',
            release_date=_DATE_3,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER.value,
            incarceration_type=StateIncarcerationType.COUNTY_JAIL.value,
        )
        ip_3 = schema.StateIncarcerationPeriod(
            admission_date=_DATE_3,
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.TRANSFER.value,
            admission_reason_raw_text='INT',
            release_date=_DATE_4,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER.value,
            incarceration_type=StateIncarcerationType.STATE_PRISON.value,
        )
        ip_4 = schema.StateIncarcerationPeriod(
            admission_date=_DATE_4,
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.TRANSFER.value,
            admission_reason_raw_text='INT',
            release_date=_DATE_5,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER.value,
            incarceration_type=StateIncarcerationType.COUNTY_JAIL.value,
        )

        expected_ip = attr.evolve(
            self.to_entity(ip),
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_reason=
            StateIncarcerationPeriodReleaseReason.
            RELEASED_FROM_TEMPORARY_CUSTODY)
        expected_ip_2 = attr.evolve(
            self.to_entity(ip_2),
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_reason=
            StateIncarcerationPeriodReleaseReason.
            RELEASED_FROM_TEMPORARY_CUSTODY)
        expected_ip_3 = attr.evolve(
            self.to_entity(ip_3),
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION)
        expected_ip_4 = attr.evolve(self.to_entity(ip_4))

        ips = [ip_2, ip_4, ip, ip_3]
        expected_ips = [
            expected_ip, expected_ip_2, expected_ip_3, expected_ip_4]

        overrides = self.create_fake_nd_region().get_enum_overrides()

        # Act
        _update_temporary_holds_helper(ips, overrides)

        # Assert
        entity_ips = [self.to_entity(ip) for ip in ips]
        self.assertCountEqual(entity_ips, expected_ips)

    def test_transformToHolds_takeAdmissionReasonFromConsecutive(self):
        # Arrange
        # Too long of a time gap between date_1 and date_2 to be
        # considered consecutive
        date_1 = _DATE_1
        date_2 = date_1 + datetime.timedelta(days=3)
        date_3 = date_2 + datetime.timedelta(days=2)
        ip = schema.StateIncarcerationPeriod(
            admission_date=date_1,
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION.value,
            admission_reason_raw_text='PV',
            release_date=date_1,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER.value,
            incarceration_type=StateIncarcerationType.EXTERNAL_UNKNOWN.value
        )
        ip_2 = schema.StateIncarcerationPeriod(
            admission_date=date_2,
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION.value,
            release_date=date_2,
            admission_reason_raw_text='ADM',
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER.value,
            incarceration_type=StateIncarcerationType.COUNTY_JAIL.value,
        )
        ip_3 = schema.StateIncarcerationPeriod(
            admission_date=date_3,
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.TRANSFER.value,
            admission_reason_raw_text='INT',
            release_date=date_3,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER.value,
            incarceration_type=StateIncarcerationType.STATE_PRISON.value,
        )

        expected_ip = attr.evolve(
            self.to_entity(ip),
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_reason=
            StateIncarcerationPeriodReleaseReason.
            RELEASED_FROM_TEMPORARY_CUSTODY)
        expected_ip_2 = attr.evolve(
            self.to_entity(ip_2),
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_reason=
            StateIncarcerationPeriodReleaseReason.
            RELEASED_FROM_TEMPORARY_CUSTODY)
        expected_ip_3 = attr.evolve(
            self.to_entity(ip_3),
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION)

        ips = [ip_2, ip, ip_3]
        expected_ips = [expected_ip, expected_ip_2, expected_ip_3]

        overrides = self.create_fake_nd_region().get_enum_overrides()

        # Act
        _update_temporary_holds_helper(ips, overrides)

        # Assert
        entity_ips = [self.to_entity(ip) for ip in ips]
        self.assertCountEqual(entity_ips, expected_ips)

    def test_transformToHolds_nonTransferReason(self):
        # Arrange
        ip = schema.StateIncarcerationPeriod(
            admission_date=_DATE_1,
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION.value,
            admission_reason_raw_text='PV',
            release_date=_DATE_2,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER.value,
            incarceration_type=StateIncarcerationType.COUNTY_JAIL.value,
        )

        ip_2 = schema.StateIncarcerationPeriod(
            admission_date=_DATE_2,
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION.value,
            release_date=_DATE_3,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER.value,
            incarceration_type=StateIncarcerationType.STATE_PRISON.value,
        )

        expected_ip = attr.evolve(
            self.to_entity(ip),
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_reason=
            StateIncarcerationPeriodReleaseReason.
            RELEASED_FROM_TEMPORARY_CUSTODY)
        expected_ip_2 = attr.evolve(self.to_entity(ip_2))
        ips = [ip, ip_2]
        expected_ips = [expected_ip, expected_ip_2]

        overrides = self.create_fake_nd_region().get_enum_overrides()

        # Act
        _update_temporary_holds_helper(ips, overrides)

        # Assert
        entity_ips = [self.to_entity(ip) for ip in ips]
        self.assertCountEqual(entity_ips, expected_ips)

    def test_associateSvrsWithIps(self):
        # Arrange
        ip_1 = StateIncarcerationPeriod.new_with_defaults(
            admission_date=_DATE_1,
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION)
        ip_2 = StateIncarcerationPeriod.new_with_defaults(
            admission_date=_DATE_2,
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION)
        placeholder_is = StateIncarcerationSentence.new_with_defaults(
            incarceration_periods=[ip_1, ip_2])

        svr_1 = StateSupervisionViolationResponse.new_with_defaults(
            response_date=_DATE_1 - datetime.timedelta(days=1),
            revocation_type=StateSupervisionViolationResponseRevocationType.
            REINCARCERATION)
        svr_2 = StateSupervisionViolationResponse.new_with_defaults(
            response_date=_DATE_2 + datetime.timedelta(days=1),
            revocation_type=StateSupervisionViolationResponseRevocationType.
            REINCARCERATION)
        svr_3 = StateSupervisionViolationResponse.new_with_defaults(
            response_date=_DATE_2 + datetime.timedelta(days=30),
            revocation_type=StateSupervisionViolationResponseRevocationType.
            RETURN_TO_SUPERVISION)
        placeholder_sv = StateSupervisionViolation.new_with_defaults(
            supervision_violation_responses=[svr_1, svr_2, svr_3])
        placeholder_sp = StateSupervisionPeriod.new_with_defaults(
            supervision_violations=[placeholder_sv])
        placeholder_ss = StateSupervisionSentence.new_with_defaults(
            supervision_periods=[placeholder_sp])

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

        expected_ip_1 = attr.evolve(
            ip_1, source_supervision_violation_response=expected_svr_1)
        expected_ip_2 = attr.evolve(
            ip_2, source_supervision_violation_response=expected_svr_2)

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
        input_people = \
            converter.convert_entity_people_to_schema_people(
                [person_without_revocation, placeholder_person])
        associate_revocation_svrs_with_ips(input_people)

        # Assert
        self.assert_people_match(
            [expected_person_without_revocation, expected_placeholder_person],
            input_people)

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
            ip_2, source_supervision_violation_response=expected_svr_1)

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
        input_people = \
            converter.convert_entity_people_to_schema_people(
                [placeholder_person])
        associate_revocation_svrs_with_ips(input_people)

        # Assert
        self.assert_people_match(
            [expected_placeholder_person],
            input_people)

    def test_associateSvrsWithIps_within90Days(self):
        # Arrange
        svr_1 = StateSupervisionViolationResponse.new_with_defaults(
            response_date=_DATE_2 + datetime.timedelta(days=1),
            revocation_type=StateSupervisionViolationResponseRevocationType.
            REINCARCERATION)
        svr_2 = StateSupervisionViolationResponse.new_with_defaults(
            response_date=_DATE_4 + datetime.timedelta(days=100),
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
        expected_svr_2 = attr.evolve(svr_2)
        expected_placeholder_sv = attr.evolve(
            placeholder_sv,
            supervision_violation_responses=[expected_svr_1, expected_svr_2])
        expected_placeholder_sp = attr.evolve(
            placeholder_sp, supervision_violations=[expected_placeholder_sv])
        expected_placeholder_ss = attr.evolve(
            placeholder_ss, supervision_periods=[expected_placeholder_sp])

        expected_ip_1 = attr.evolve(
            ip_1, source_supervision_violation_response=expected_svr_1)
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
        input_people = \
            converter.convert_entity_people_to_schema_people(
                [placeholder_person])
        associate_revocation_svrs_with_ips(input_people)

        # Assert
        self.assert_people_match(
            [expected_placeholder_person],
            input_people)

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
            ip_1, source_supervision_violation_response=expected_svr_1)
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
        input_people = \
            converter.convert_entity_people_to_schema_people(
                [placeholder_person])
        associate_revocation_svrs_with_ips(input_people)

        # Assert
        self.assert_people_match(
            [expected_placeholder_person],
            input_people)

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
