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
"""Tests for state_period_matching_utils.py"""
import datetime


from recidiviz.persistence.entity_matching.state.state_period_matching_utils import (
    add_supervising_officer_to_open_supervision_periods,
)
from recidiviz.tests.persistence.database.schema.state.schema_test_utils import (
    generate_agent,
    generate_person,
    generate_external_id,
    generate_supervision_period,
    generate_supervision_sentence,
    generate_sentence_group,
)
from recidiviz.tests.persistence.entity_matching.state.base_state_entity_matcher_test_classes import (
    BaseStateMatchingUtilsTest,
)

_DATE_1 = datetime.date(year=2019, month=1, day=1)
_DATE_2 = datetime.date(year=2019, month=2, day=1)
_DATE_3 = datetime.date(year=2019, month=3, day=1)
_DATE_4 = datetime.date(year=2019, month=4, day=1)
_EXTERNAL_ID = "EXTERNAL_ID-1"
_EXTERNAL_ID_2 = "EXTERNAL_ID-2"
_EXTERNAL_ID_3 = "EXTERNAL_ID-3"
_ID = 1
_ID_2 = 2
_ID_3 = 3
_STATE_CODE = "US_XX"
_ID_TYPE = "ID_TYPE"


# pylint: disable=protected-access
class TestStatePeriodMatchingUtils(BaseStateMatchingUtilsTest):
    """Tests for state period matching utils"""

    def test_addSupervisingOfficerToOpenSupervisionPeriods(self) -> None:
        # Arrange
        supervising_officer = generate_agent(
            agent_id=_ID, external_id=_EXTERNAL_ID, state_code=_STATE_CODE
        )
        person = generate_person(person_id=_ID, supervising_officer=supervising_officer)
        external_id = generate_external_id(
            person_external_id_id=_ID,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            id_type=_ID_TYPE,
        )
        open_supervision_period = generate_supervision_period(
            person=person,
            supervision_period_id=_ID,
            external_id=_EXTERNAL_ID,
            start_date=_DATE_1,
            state_code=_STATE_CODE,
        )
        placeholder_supervision_period = generate_supervision_period(
            person=person,
            supervision_period_id=_ID_2,
            state_code=_STATE_CODE,
        )
        closed_supervision_period = generate_supervision_period(
            person=person,
            supervision_period_id=_ID_3,
            external_id=_EXTERNAL_ID_3,
            start_date=_DATE_3,
            termination_date=_DATE_4,
            state_code=_STATE_CODE,
        )
        supervision_sentence = generate_supervision_sentence(
            person=person,
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            supervision_sentence_id=_ID,
            supervision_periods=[
                open_supervision_period,
                placeholder_supervision_period,
                closed_supervision_period,
            ],
        )
        sentence_group = generate_sentence_group(
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            sentence_group_id=_ID,
            supervision_sentences=[supervision_sentence],
        )
        person.external_ids = [external_id]
        person.sentence_groups = [sentence_group]

        # Act
        add_supervising_officer_to_open_supervision_periods([person])

        # Assert
        self.assertEqual(
            open_supervision_period.supervising_officer, supervising_officer
        )
        self.assertIsNone(
            placeholder_supervision_period.supervising_officer, supervising_officer
        )
        self.assertIsNone(
            closed_supervision_period.supervising_officer, supervising_officer
        )
