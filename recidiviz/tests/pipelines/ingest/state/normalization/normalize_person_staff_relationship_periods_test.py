# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Tests for normalize_person_staff_relationship_periods.py"""
import datetime
import unittest

from recidiviz.common.constants.state.state_person_staff_relationship_period import (
    StatePersonStaffRelationshipType,
)
from recidiviz.common.constants.state.state_system_type import StateSystemType
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import (
    StatePersonStaffRelationshipPeriod,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStatePersonStaffRelationshipPeriod,
)
from recidiviz.pipelines.ingest.state.normalization.normalize_person_staff_relationship_periods import (
    get_normalized_person_staff_relationship_periods,
)

_STAFF_ID_1 = 1000
_STAFF_ID_1_EXTERNAL_ID_A = "STAFF_EXTERNAL_ID_1A"
_STAFF_ID_1_EXTERNAL_ID_B = "STAFF_EXTERNAL_ID_1B"
_STAFF_ID_2 = 2000
_STAFF_ID_2_EXTERNAL_ID = "STAFF_EXTERNAL_ID_2"

_DEFAULT_STAFF_EXTERNAL_ID_TO_STAFF_ID = {
    (_STAFF_ID_1_EXTERNAL_ID_A, "US_XX_STAFF_ID"): _STAFF_ID_1,
    (_STAFF_ID_1_EXTERNAL_ID_B, "US_XX_STAFF_ID_OTHER"): _STAFF_ID_1,
    (_STAFF_ID_2_EXTERNAL_ID, "US_XX_STAFF_ID"): _STAFF_ID_2,
}

_DATE_1 = datetime.date(2019, 1, 1)
_DATE_2 = datetime.date(2020, 1, 1)
_DATE_3 = datetime.date(2021, 1, 1)


class TestNormalizePersonStaffRelationshipPeriods(unittest.TestCase):
    """Tests the normalization functionality for state_staff_relationship_period."""

    def _staff_1_supervising_officer_period(
        self, start_date: datetime.date, end_date_exclusive: datetime.date | None
    ) -> StatePersonStaffRelationshipPeriod:
        return StatePersonStaffRelationshipPeriod(
            person_staff_relationship_period_id=123,
            state_code=StateCode.US_XX.value,
            relationship_start_date=start_date,
            relationship_end_date_exclusive=end_date_exclusive,
            system_type=StateSystemType.SUPERVISION,
            relationship_type=StatePersonStaffRelationshipType.SUPERVISING_OFFICER,
            associated_staff_external_id=_STAFF_ID_1_EXTERNAL_ID_A,
            associated_staff_external_id_type="US_XX_STAFF_ID",
        )

    def test_no_periods(self) -> None:
        relationship_periods: list[StatePersonStaffRelationshipPeriod] = []
        normalized_periods = get_normalized_person_staff_relationship_periods(
            person_id=1,
            staff_external_id_to_staff_id={},
            person_staff_relationship_periods=relationship_periods,
        )

        expected_normalized_periods: list[
            NormalizedStatePersonStaffRelationshipPeriod
        ] = []
        self.assertEqual(expected_normalized_periods, normalized_periods)

    def test_single_period(self) -> None:
        relationship_periods: list[StatePersonStaffRelationshipPeriod] = [
            StatePersonStaffRelationshipPeriod(
                person_staff_relationship_period_id=123,
                state_code=StateCode.US_XX.value,
                relationship_start_date=_DATE_1,
                relationship_end_date_exclusive=None,
                system_type=StateSystemType.SUPERVISION,
                relationship_type=StatePersonStaffRelationshipType.SUPERVISING_OFFICER,
                associated_staff_external_id=_STAFF_ID_1_EXTERNAL_ID_A,
                associated_staff_external_id_type="US_XX_STAFF_ID",
            )
        ]
        normalized_periods = get_normalized_person_staff_relationship_periods(
            person_id=1,
            staff_external_id_to_staff_id=_DEFAULT_STAFF_EXTERNAL_ID_TO_STAFF_ID,
            person_staff_relationship_periods=relationship_periods,
        )

        expected_normalized_periods = [
            NormalizedStatePersonStaffRelationshipPeriod(
                state_code="US_XX",
                system_type=StateSystemType.SUPERVISION,
                system_type_raw_text=None,
                relationship_type=StatePersonStaffRelationshipType.SUPERVISING_OFFICER,
                relationship_type_raw_text=None,
                relationship_start_date=_DATE_1,
                relationship_end_date_exclusive=None,
                location_external_id=None,
                associated_staff_external_id=_STAFF_ID_1_EXTERNAL_ID_A,
                associated_staff_external_id_type="US_XX_STAFF_ID",
                # These values are now set / updated
                person_staff_relationship_period_id=9020615632404388141,
                relationship_priority=1,
                associated_staff_id=_STAFF_ID_1,
            )
        ]
        self.assertEqual(expected_normalized_periods, normalized_periods)

    def test_two_non_overlapping_periods(self) -> None:
        relationship_periods: list[StatePersonStaffRelationshipPeriod] = [
            StatePersonStaffRelationshipPeriod(
                person_staff_relationship_period_id=123,
                state_code=StateCode.US_XX.value,
                relationship_start_date=_DATE_1,
                relationship_end_date_exclusive=_DATE_2,
                system_type=StateSystemType.SUPERVISION,
                relationship_type=StatePersonStaffRelationshipType.SUPERVISING_OFFICER,
                associated_staff_external_id=_STAFF_ID_1_EXTERNAL_ID_A,
                associated_staff_external_id_type="US_XX_STAFF_ID",
            ),
            StatePersonStaffRelationshipPeriod(
                person_staff_relationship_period_id=123,
                state_code=StateCode.US_XX.value,
                relationship_start_date=_DATE_2,
                relationship_end_date_exclusive=None,
                system_type=StateSystemType.SUPERVISION,
                relationship_type=StatePersonStaffRelationshipType.SUPERVISING_OFFICER,
                associated_staff_external_id=_STAFF_ID_2_EXTERNAL_ID,
                associated_staff_external_id_type="US_XX_STAFF_ID",
            ),
        ]
        normalized_periods = get_normalized_person_staff_relationship_periods(
            person_id=1,
            staff_external_id_to_staff_id=_DEFAULT_STAFF_EXTERNAL_ID_TO_STAFF_ID,
            person_staff_relationship_periods=relationship_periods,
        )

        expected_normalized_periods = [
            NormalizedStatePersonStaffRelationshipPeriod(
                state_code="US_XX",
                system_type=StateSystemType.SUPERVISION,
                system_type_raw_text=None,
                relationship_type=StatePersonStaffRelationshipType.SUPERVISING_OFFICER,
                relationship_type_raw_text=None,
                relationship_start_date=_DATE_1,
                relationship_end_date_exclusive=_DATE_2,
                location_external_id=None,
                associated_staff_external_id=_STAFF_ID_1_EXTERNAL_ID_A,
                associated_staff_external_id_type="US_XX_STAFF_ID",
                # These values are now set / updated
                person_staff_relationship_period_id=9046224349876569075,
                relationship_priority=1,
                associated_staff_id=_STAFF_ID_1,
            ),
            NormalizedStatePersonStaffRelationshipPeriod(
                state_code="US_XX",
                system_type=StateSystemType.SUPERVISION,
                system_type_raw_text=None,
                relationship_type=StatePersonStaffRelationshipType.SUPERVISING_OFFICER,
                relationship_type_raw_text=None,
                relationship_start_date=_DATE_2,
                relationship_end_date_exclusive=None,
                location_external_id=None,
                associated_staff_external_id=_STAFF_ID_2_EXTERNAL_ID,
                associated_staff_external_id_type="US_XX_STAFF_ID",
                # These values are now set / updated
                person_staff_relationship_period_id=9049269602583208924,
                relationship_priority=1,
                associated_staff_id=_STAFF_ID_2,
            ),
        ]
        self.assertEqual(expected_normalized_periods, normalized_periods)

    def test_overlapping_periods_different_role(self) -> None:
        relationship_periods: list[StatePersonStaffRelationshipPeriod] = [
            StatePersonStaffRelationshipPeriod(
                person_staff_relationship_period_id=123,
                state_code=StateCode.US_XX.value,
                relationship_start_date=_DATE_1,
                relationship_end_date_exclusive=_DATE_3,
                system_type=StateSystemType.SUPERVISION,
                relationship_type=StatePersonStaffRelationshipType.SUPERVISING_OFFICER,
                associated_staff_external_id=_STAFF_ID_1_EXTERNAL_ID_A,
                associated_staff_external_id_type="US_XX_STAFF_ID",
            ),
            StatePersonStaffRelationshipPeriod(
                person_staff_relationship_period_id=123,
                state_code=StateCode.US_XX.value,
                relationship_start_date=_DATE_2,
                relationship_end_date_exclusive=None,
                system_type=StateSystemType.INCARCERATION,
                relationship_type=StatePersonStaffRelationshipType.CASE_MANAGER,
                associated_staff_external_id=_STAFF_ID_2_EXTERNAL_ID,
                associated_staff_external_id_type="US_XX_STAFF_ID",
            ),
        ]
        normalized_periods = get_normalized_person_staff_relationship_periods(
            person_id=1,
            staff_external_id_to_staff_id=_DEFAULT_STAFF_EXTERNAL_ID_TO_STAFF_ID,
            person_staff_relationship_periods=relationship_periods,
        )

        expected_normalized_periods = [
            NormalizedStatePersonStaffRelationshipPeriod(
                state_code="US_XX",
                system_type=StateSystemType.SUPERVISION,
                system_type_raw_text=None,
                relationship_type=StatePersonStaffRelationshipType.SUPERVISING_OFFICER,
                relationship_type_raw_text=None,
                relationship_start_date=_DATE_1,
                relationship_end_date_exclusive=_DATE_3,
                location_external_id=None,
                associated_staff_external_id=_STAFF_ID_1_EXTERNAL_ID_A,
                associated_staff_external_id_type="US_XX_STAFF_ID",
                # These values are now set / updated
                person_staff_relationship_period_id=9041585990007638604,
                relationship_priority=1,
                associated_staff_id=_STAFF_ID_1,
            ),
            NormalizedStatePersonStaffRelationshipPeriod(
                state_code="US_XX",
                system_type=StateSystemType.INCARCERATION,
                system_type_raw_text=None,
                relationship_type=StatePersonStaffRelationshipType.CASE_MANAGER,
                relationship_type_raw_text=None,
                relationship_start_date=_DATE_2,
                relationship_end_date_exclusive=None,
                location_external_id=None,
                associated_staff_external_id=_STAFF_ID_2_EXTERNAL_ID,
                associated_staff_external_id_type="US_XX_STAFF_ID",
                # These values are now set / updated
                person_staff_relationship_period_id=9032608605326273643,
                relationship_priority=1,
                associated_staff_id=_STAFF_ID_2,
            ),
        ]
        self.assertEqual(expected_normalized_periods, normalized_periods)

    def test_overlapping_periods_same_role(self) -> None:
        relationship_periods: list[StatePersonStaffRelationshipPeriod] = [
            StatePersonStaffRelationshipPeriod(
                person_staff_relationship_period_id=123,
                state_code=StateCode.US_XX.value,
                relationship_start_date=_DATE_1,
                relationship_end_date_exclusive=_DATE_3,
                system_type=StateSystemType.SUPERVISION,
                relationship_type=StatePersonStaffRelationshipType.SUPERVISING_OFFICER,
                associated_staff_external_id=_STAFF_ID_1_EXTERNAL_ID_A,
                associated_staff_external_id_type="US_XX_STAFF_ID",
            ),
            StatePersonStaffRelationshipPeriod(
                person_staff_relationship_period_id=123,
                state_code=StateCode.US_XX.value,
                relationship_start_date=_DATE_2,
                relationship_end_date_exclusive=None,
                system_type=StateSystemType.SUPERVISION,
                relationship_type=StatePersonStaffRelationshipType.SUPERVISING_OFFICER,
                associated_staff_external_id=_STAFF_ID_2_EXTERNAL_ID,
                associated_staff_external_id_type="US_XX_STAFF_ID",
            ),
        ]
        normalized_periods = get_normalized_person_staff_relationship_periods(
            person_id=1,
            staff_external_id_to_staff_id=_DEFAULT_STAFF_EXTERNAL_ID_TO_STAFF_ID,
            person_staff_relationship_periods=relationship_periods,
        )

        # Each period gets split into two and the overlapping section is prioritized by
        # start date and assigned sequence numbers accordingly.
        expected_normalized_periods = [
            NormalizedStatePersonStaffRelationshipPeriod(
                state_code="US_XX",
                system_type=StateSystemType.SUPERVISION,
                system_type_raw_text=None,
                relationship_type=StatePersonStaffRelationshipType.SUPERVISING_OFFICER,
                relationship_type_raw_text=None,
                relationship_start_date=datetime.date(2019, 1, 1),
                relationship_end_date_exclusive=datetime.date(2020, 1, 1),
                location_external_id=None,
                relationship_priority=1,
                associated_staff_external_id="STAFF_EXTERNAL_ID_1A",
                associated_staff_external_id_type="US_XX_STAFF_ID",
                person_staff_relationship_period_id=9046224349876569075,
                person=None,
                associated_staff_id=_STAFF_ID_1,
            ),
            NormalizedStatePersonStaffRelationshipPeriod(
                state_code="US_XX",
                system_type=StateSystemType.SUPERVISION,
                system_type_raw_text=None,
                relationship_type=StatePersonStaffRelationshipType.SUPERVISING_OFFICER,
                relationship_type_raw_text=None,
                relationship_start_date=datetime.date(2020, 1, 1),
                relationship_end_date_exclusive=datetime.date(2021, 1, 1),
                location_external_id=None,
                relationship_priority=1,
                associated_staff_external_id="STAFF_EXTERNAL_ID_2",
                associated_staff_external_id_type="US_XX_STAFF_ID",
                person_staff_relationship_period_id=9059851427843950549,
                person=None,
                associated_staff_id=_STAFF_ID_2,
            ),
            NormalizedStatePersonStaffRelationshipPeriod(
                state_code="US_XX",
                system_type=StateSystemType.SUPERVISION,
                system_type_raw_text=None,
                relationship_type=StatePersonStaffRelationshipType.SUPERVISING_OFFICER,
                relationship_type_raw_text=None,
                relationship_start_date=datetime.date(2020, 1, 1),
                relationship_end_date_exclusive=datetime.date(2021, 1, 1),
                location_external_id=None,
                relationship_priority=2,
                associated_staff_external_id="STAFF_EXTERNAL_ID_1A",
                associated_staff_external_id_type="US_XX_STAFF_ID",
                person_staff_relationship_period_id=9001502564037020168,
                person=None,
                associated_staff_id=_STAFF_ID_1,
            ),
            NormalizedStatePersonStaffRelationshipPeriod(
                state_code="US_XX",
                system_type=StateSystemType.SUPERVISION,
                system_type_raw_text=None,
                relationship_type=StatePersonStaffRelationshipType.SUPERVISING_OFFICER,
                relationship_type_raw_text=None,
                relationship_start_date=datetime.date(2021, 1, 1),
                relationship_end_date_exclusive=None,
                location_external_id=None,
                relationship_priority=1,
                associated_staff_external_id="STAFF_EXTERNAL_ID_2",
                associated_staff_external_id_type="US_XX_STAFF_ID",
                person_staff_relationship_period_id=9049410191367390553,
                person=None,
                associated_staff_id=_STAFF_ID_2,
            ),
        ]
        self.assertEqual(expected_normalized_periods, normalized_periods)

    def test_overlapping_periods_same_role_same_officer(self) -> None:
        relationship_periods: list[StatePersonStaffRelationshipPeriod] = [
            StatePersonStaffRelationshipPeriod(
                person_staff_relationship_period_id=123,
                state_code=StateCode.US_XX.value,
                relationship_start_date=_DATE_1,
                relationship_end_date_exclusive=_DATE_3,
                system_type=StateSystemType.SUPERVISION,
                relationship_type=StatePersonStaffRelationshipType.SUPERVISING_OFFICER,
                relationship_type_raw_text="PAROLE_OFFICER",
                associated_staff_external_id=_STAFF_ID_1_EXTERNAL_ID_A,
                associated_staff_external_id_type="US_XX_STAFF_ID",
            ),
            StatePersonStaffRelationshipPeriod(
                person_staff_relationship_period_id=123,
                state_code=StateCode.US_XX.value,
                relationship_start_date=_DATE_2,
                relationship_end_date_exclusive=None,
                system_type=StateSystemType.SUPERVISION,
                relationship_type=StatePersonStaffRelationshipType.SUPERVISING_OFFICER,
                relationship_type_raw_text="PROBATION_OFFICER",
                associated_staff_external_id=_STAFF_ID_1_EXTERNAL_ID_A,
                associated_staff_external_id_type="US_XX_STAFF_ID",
            ),
        ]
        normalized_periods = get_normalized_person_staff_relationship_periods(
            person_id=1,
            staff_external_id_to_staff_id=_DEFAULT_STAFF_EXTERNAL_ID_TO_STAFF_ID,
            person_staff_relationship_periods=relationship_periods,
        )

        # Each period gets split into two and for the overlapping section we only
        # produce one period because both overlapping parts are for the same staff
        # member.
        expected_normalized_periods = [
            NormalizedStatePersonStaffRelationshipPeriod(
                state_code="US_XX",
                system_type=StateSystemType.SUPERVISION,
                system_type_raw_text=None,
                relationship_type=StatePersonStaffRelationshipType.SUPERVISING_OFFICER,
                relationship_type_raw_text="PAROLE_OFFICER",
                relationship_start_date=datetime.date(2019, 1, 1),
                relationship_end_date_exclusive=datetime.date(2020, 1, 1),
                location_external_id=None,
                relationship_priority=1,
                associated_staff_external_id="STAFF_EXTERNAL_ID_1A",
                associated_staff_external_id_type="US_XX_STAFF_ID",
                person_staff_relationship_period_id=9027146516096144398,
                person=None,
                associated_staff_id=_STAFF_ID_1,
            ),
            NormalizedStatePersonStaffRelationshipPeriod(
                state_code="US_XX",
                system_type=StateSystemType.SUPERVISION,
                system_type_raw_text=None,
                relationship_type=StatePersonStaffRelationshipType.SUPERVISING_OFFICER,
                relationship_type_raw_text="PROBATION_OFFICER",
                relationship_start_date=datetime.date(2020, 1, 1),
                relationship_end_date_exclusive=datetime.date(2021, 1, 1),
                location_external_id=None,
                relationship_priority=1,
                associated_staff_external_id="STAFF_EXTERNAL_ID_1A",
                associated_staff_external_id_type="US_XX_STAFF_ID",
                person_staff_relationship_period_id=9020702918296250850,
                person=None,
                associated_staff_id=_STAFF_ID_1,
            ),
            NormalizedStatePersonStaffRelationshipPeriod(
                state_code="US_XX",
                system_type=StateSystemType.SUPERVISION,
                system_type_raw_text=None,
                relationship_type=StatePersonStaffRelationshipType.SUPERVISING_OFFICER,
                relationship_type_raw_text="PROBATION_OFFICER",
                relationship_start_date=datetime.date(2021, 1, 1),
                relationship_end_date_exclusive=None,
                location_external_id=None,
                relationship_priority=1,
                associated_staff_external_id="STAFF_EXTERNAL_ID_1A",
                associated_staff_external_id_type="US_XX_STAFF_ID",
                person_staff_relationship_period_id=9062713401321016027,
                person=None,
                associated_staff_id=_STAFF_ID_1,
            ),
        ]
        self.assertEqual(expected_normalized_periods, normalized_periods)

    def test_overlapping_periods_same_role_priorities_already_set(self) -> None:
        relationship_periods: list[StatePersonStaffRelationshipPeriod] = [
            StatePersonStaffRelationshipPeriod(
                person_staff_relationship_period_id=123,
                state_code=StateCode.US_XX.value,
                relationship_start_date=_DATE_1,
                relationship_end_date_exclusive=_DATE_3,
                system_type=StateSystemType.SUPERVISION,
                relationship_type=StatePersonStaffRelationshipType.SUPERVISING_OFFICER,
                associated_staff_external_id=_STAFF_ID_1_EXTERNAL_ID_A,
                associated_staff_external_id_type="US_XX_STAFF_ID",
                relationship_priority=1,
            ),
            StatePersonStaffRelationshipPeriod(
                person_staff_relationship_period_id=123,
                state_code=StateCode.US_XX.value,
                relationship_start_date=_DATE_2,
                relationship_end_date_exclusive=None,
                system_type=StateSystemType.SUPERVISION,
                relationship_type=StatePersonStaffRelationshipType.SUPERVISING_OFFICER,
                associated_staff_external_id=_STAFF_ID_2_EXTERNAL_ID,
                associated_staff_external_id_type="US_XX_STAFF_ID",
                relationship_priority=2,
            ),
        ]
        normalized_periods = get_normalized_person_staff_relationship_periods(
            person_id=1,
            staff_external_id_to_staff_id=_DEFAULT_STAFF_EXTERNAL_ID_TO_STAFF_ID,
            person_staff_relationship_periods=relationship_periods,
        )

        # Each period gets split into two and the overlapping section is prioritized by
        # start date and assigned sequence numbers accordingly.
        expected_normalized_periods = [
            NormalizedStatePersonStaffRelationshipPeriod(
                state_code="US_XX",
                system_type=StateSystemType.SUPERVISION,
                system_type_raw_text=None,
                relationship_type=StatePersonStaffRelationshipType.SUPERVISING_OFFICER,
                relationship_type_raw_text=None,
                relationship_start_date=datetime.date(2019, 1, 1),
                relationship_end_date_exclusive=datetime.date(2020, 1, 1),
                location_external_id=None,
                relationship_priority=1,
                associated_staff_external_id="STAFF_EXTERNAL_ID_1A",
                associated_staff_external_id_type="US_XX_STAFF_ID",
                person_staff_relationship_period_id=9046224349876569075,
                person=None,
                associated_staff_id=_STAFF_ID_1,
            ),
            NormalizedStatePersonStaffRelationshipPeriod(
                state_code="US_XX",
                system_type=StateSystemType.SUPERVISION,
                system_type_raw_text=None,
                relationship_type=StatePersonStaffRelationshipType.SUPERVISING_OFFICER,
                relationship_type_raw_text=None,
                relationship_start_date=datetime.date(2020, 1, 1),
                relationship_end_date_exclusive=datetime.date(2021, 1, 1),
                location_external_id=None,
                relationship_priority=1,
                associated_staff_external_id=_STAFF_ID_1_EXTERNAL_ID_A,
                associated_staff_external_id_type="US_XX_STAFF_ID",
                person_staff_relationship_period_id=9038545197534851514,
                person=None,
                associated_staff_id=_STAFF_ID_1,
            ),
            NormalizedStatePersonStaffRelationshipPeriod(
                state_code="US_XX",
                system_type=StateSystemType.SUPERVISION,
                system_type_raw_text=None,
                relationship_type=StatePersonStaffRelationshipType.SUPERVISING_OFFICER,
                relationship_type_raw_text=None,
                relationship_start_date=datetime.date(2020, 1, 1),
                relationship_end_date_exclusive=datetime.date(2021, 1, 1),
                location_external_id=None,
                # This is lower priority even though it's for a period that started
                # earlier because this was the designated priority on the input entity
                relationship_priority=2,
                associated_staff_external_id=_STAFF_ID_2_EXTERNAL_ID,
                associated_staff_external_id_type="US_XX_STAFF_ID",
                person_staff_relationship_period_id=9063398130978585492,
                person=None,
                associated_staff_id=_STAFF_ID_2,
            ),
            NormalizedStatePersonStaffRelationshipPeriod(
                state_code="US_XX",
                system_type=StateSystemType.SUPERVISION,
                system_type_raw_text=None,
                relationship_type=StatePersonStaffRelationshipType.SUPERVISING_OFFICER,
                relationship_type_raw_text=None,
                relationship_start_date=datetime.date(2021, 1, 1),
                relationship_end_date_exclusive=None,
                location_external_id=None,
                relationship_priority=1,
                associated_staff_external_id=_STAFF_ID_2_EXTERNAL_ID,
                associated_staff_external_id_type="US_XX_STAFF_ID",
                person_staff_relationship_period_id=9049410191367390553,
                person=None,
                associated_staff_id=_STAFF_ID_2,
            ),
        ]
        self.assertEqual(expected_normalized_periods, normalized_periods)
