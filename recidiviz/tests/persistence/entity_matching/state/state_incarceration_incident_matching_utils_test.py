# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Tests for state_incarceration_incident_matching_utils.py"""
import datetime

import attr

from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodStatus,
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.database.schema_entity_converter.schema_entity_converter import (
    convert_entity_to_schema_object,
)
from recidiviz.persistence.entity.state.entities import (
    StatePerson,
    StateSentenceGroup,
    StateIncarcerationPeriod,
    StateIncarcerationIncident,
    StateIncarcerationSentence,
)
from recidiviz.persistence.entity_matching.state.state_incarceration_incident_matching_utils import (
    move_incidents_onto_periods,
)
from recidiviz.tests.persistence.entity_matching.state.base_state_entity_matcher_test_classes import (
    BaseStateMatchingUtilsTest,
)


_DATE_1 = datetime.date(year=2019, month=1, day=1)
_DATE_2 = datetime.date(year=2019, month=2, day=1)
_DATE_3 = datetime.date(year=2019, month=3, day=1)
_DATE_4 = datetime.date(year=2019, month=4, day=1)
_DATE_5 = datetime.date(year=2019, month=5, day=1)
_DATE_6 = datetime.date(year=2019, month=6, day=1)
_DATE_7 = datetime.date(year=2019, month=7, day=1)
_DATE_8 = datetime.date(year=2019, month=8, day=1)
_EXTERNAL_ID = "EXTERNAL_ID-1"
_EXTERNAL_ID_2 = "EXTERNAL_ID-2"
_EXTERNAL_ID_3 = "EXTERNAL_ID-3"
_EXTERNAL_ID_4 = "EXTERNAL_ID-4"
_EXTERNAL_ID_5 = "EXTERNAL_ID-5"
_FACILITY = "FACILITY"
_FACILITY_2 = "FACILITY_2"
_FACILITY_3 = "FACILITY_3"
_FACILITY_4 = "FACILITY_4"
_STATE_CODE = "US_XX"


# pylint: disable=protected-access
class TestIncidentMatchingUtils(BaseStateMatchingUtilsTest):
    """Test class for US_ND specific matching utils."""

    def test_moveIncidentsOntoPeriods(self) -> None:
        merged_incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID + "|" + _EXTERNAL_ID_2,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            facility=_FACILITY,
            admission_date=_DATE_1,
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=_DATE_3,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )
        merged_incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_3 + "|" + _EXTERNAL_ID_4,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            facility=_FACILITY_2,
            admission_date=_DATE_3,
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=_DATE_5,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )
        unmerged_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_5,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            facility=_FACILITY_3,
            admission_date=_DATE_5,
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
        )

        incident_1 = StateIncarcerationIncident.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            facility=_FACILITY,
            incident_date=_DATE_2,
        )
        incident_2 = StateIncarcerationIncident.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_2,
            facility=_FACILITY_2,
            incident_date=_DATE_4,
        )
        incident_3 = StateIncarcerationIncident.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_3,
            facility=_FACILITY_4,
            incident_date=_DATE_7,
        )
        placeholder_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            incarceration_incidents=[incident_1, incident_2, incident_3],
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_periods=[
                merged_incarceration_period_1,
                merged_incarceration_period_2,
                unmerged_incarceration_period,
            ],
        )
        placeholder_incarceration_sentence = (
            StateIncarcerationSentence.new_with_defaults(
                state_code=_STATE_CODE,
                external_id=_EXTERNAL_ID_2,
                incarceration_periods=[placeholder_incarceration_period],
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            )
        )
        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_sentences=[
                incarceration_sentence,
                placeholder_incarceration_sentence,
            ],
        )

        person = StatePerson.new_with_defaults(
            state_code=_STATE_CODE, sentence_groups=[sentence_group]
        )

        expected_merged_period = attr.evolve(
            merged_incarceration_period_1, incarceration_incidents=[incident_1]
        )
        expected_merged_period_2 = attr.evolve(
            merged_incarceration_period_2, incarceration_incidents=[incident_2]
        )
        expected_unmerged_period = attr.evolve(unmerged_incarceration_period)
        expected_placeholder_period = attr.evolve(
            placeholder_incarceration_period, incarceration_incidents=[incident_3]
        )
        expected_sentence = attr.evolve(
            incarceration_sentence,
            incarceration_periods=[
                expected_merged_period,
                expected_merged_period_2,
                expected_unmerged_period,
            ],
        )
        expected_placeholder_sentence = attr.evolve(
            placeholder_incarceration_sentence,
            incarceration_periods=[expected_placeholder_period],
        )
        expected_sentence_group = attr.evolve(
            sentence_group,
            incarceration_sentences=[expected_sentence, expected_placeholder_sentence],
        )
        expected_person = convert_entity_to_schema_object(
            attr.evolve(person, sentence_groups=[expected_sentence_group])
        )

        schema_person = convert_entity_to_schema_object(person)
        if not isinstance(schema_person, schema.StatePerson):
            self.fail(f"Expected schema.StatePerson. Found {schema_person}")

        move_incidents_onto_periods([schema_person])
        self.assert_schema_objects_equal(expected_person, schema_person)
