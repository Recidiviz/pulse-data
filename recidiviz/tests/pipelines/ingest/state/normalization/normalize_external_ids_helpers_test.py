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
"""Unittests for normalize_external_ids_helpers.py"""
import datetime
import unittest

from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import StatePersonExternalId
from recidiviz.pipelines.ingest.state.normalization.normalize_external_ids_helpers import (
    select_alphabetically_highest_person_external_id,
    select_most_recently_active_person_external_id,
    select_single_external_id_with_is_current_display_id,
)

_ID_TYPE = "US_XX_ID_TYPE"
_ID_TYPE_2 = "US_XX_ID_TYPE_2"


class TestSelectAlphabeticallyHighestPersonExternalId(unittest.TestCase):
    """Unittests for the select_alphabetically_highest_person_external_id() helper in
    normalize_external_ids_helpers.py"""

    def make_external_id(
        self, *, external_id: str, id_type: str = _ID_TYPE
    ) -> StatePersonExternalId:
        return StatePersonExternalId(
            person_external_id_id=1,
            state_code=StateCode.US_XX.value,
            external_id=external_id,
            id_type=id_type,
            is_current_display_id_for_type=None,
            id_active_from_datetime=None,
            id_active_to_datetime=None,
        )

    def test_single(self) -> None:
        ids = [
            self.make_external_id(external_id="ABC123"),
        ]
        result = select_alphabetically_highest_person_external_id(ids)
        self.assertEqual(result.external_id, "ABC123")

    def test_returns_highest_alphabetical_id(self) -> None:
        ids = [
            self.make_external_id(external_id="ABC123"),
            self.make_external_id(external_id="ZZZ999"),
            self.make_external_id(external_id="LMN456"),
        ]
        result = select_alphabetically_highest_person_external_id(ids)
        self.assertEqual(result.external_id, "ZZZ999")

    def test_returns_highest_alphabetical_id_numerical_strings(self) -> None:
        ids = [
            self.make_external_id(external_id="8"),
            self.make_external_id(external_id="9"),
            self.make_external_id(external_id="10"),
        ]
        result = select_alphabetically_highest_person_external_id(ids)
        self.assertEqual(result.external_id, "9")

    def test_raises_on_empty_list(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Cannot call select_alphabetically_highest_person_external_id\(\) with an "
            r"empty external_ids list",
        ):
            select_alphabetically_highest_person_external_id([])

    def test_raises_on_multiple_id_types(self) -> None:
        ids = [
            self.make_external_id(external_id="ABC123", id_type=_ID_TYPE),
            self.make_external_id(external_id="DEF456", id_type=_ID_TYPE_2),
        ]
        with self.assertRaisesRegex(
            ValueError, r"Found multiple id_types in the provided external_ids list"
        ):
            _ = select_alphabetically_highest_person_external_id(ids)

    def test_raises_on_equal_external_ids(self) -> None:
        id1 = self.make_external_id(external_id="SAME123")
        id2 = self.make_external_id(external_id="SAME123")
        with self.assertRaisesRegex(
            ValueError,
            r"Found multiple external ids with external_id \[SAME123\] and id_type \[US_XX_ID_TYPE\]",
        ):
            _ = select_alphabetically_highest_person_external_id([id1, id2])


class TestSelectMostRecentlyActivePersonExternalId(unittest.TestCase):
    """Unittests for the select_most_recently_active_person_external_id() helper in
    normalize_external_ids_helpers.py"""

    @staticmethod
    def make_external_id(
        *,
        external_id: str,
        id_active_from_datetime: datetime.datetime | None,
        id_active_to_datetime: datetime.datetime | None = None,
        id_type: str = _ID_TYPE
    ) -> StatePersonExternalId:
        return StatePersonExternalId(
            person_external_id_id=1,
            state_code=StateCode.US_XX.value,
            external_id=external_id,
            id_type=id_type,
            is_current_display_id_for_type=None,
            id_active_from_datetime=id_active_from_datetime,
            id_active_to_datetime=id_active_to_datetime,
        )

    def test_single(self) -> None:
        ids = [
            self.make_external_id(
                external_id="ABC123",
                id_active_from_datetime=datetime.datetime(2020, 1, 1),
            ),
        ]
        result = select_most_recently_active_person_external_id(ids)
        self.assertEqual(result.external_id, "ABC123")

    def test_only_active_from_dates_set(self) -> None:
        ids = [
            self.make_external_id(
                external_id="ABC123",
                id_active_from_datetime=datetime.datetime(2020, 2, 2),
            ),
            self.make_external_id(
                external_id="ZZZ999",
                id_active_from_datetime=datetime.datetime(2020, 1, 1),
            ),
            self.make_external_id(
                external_id="LMN456",
                id_active_from_datetime=datetime.datetime(2020, 3, 3),
            ),
        ]
        result = select_most_recently_active_person_external_id(ids)
        self.assertEqual(result.external_id, "LMN456")

    def test_active_from_and_active_to_dates_set(self) -> None:
        ids = [
            self.make_external_id(
                external_id="ABC123",
                id_active_from_datetime=datetime.datetime(2020, 1, 1),
                id_active_to_datetime=datetime.datetime(2020, 2, 2),
            ),
            # Pick this ID because it's ongoing
            self.make_external_id(
                external_id="ZZZ999",
                id_active_from_datetime=datetime.datetime(2020, 1, 1),
            ),
            self.make_external_id(
                external_id="LMN456",
                id_active_from_datetime=datetime.datetime(2019, 3, 3),
                id_active_to_datetime=datetime.datetime(2020, 2, 2),
            ),
        ]
        result = select_most_recently_active_person_external_id(ids)
        self.assertEqual(result.external_id, "ZZZ999")

    def test_active_from_and_active_to_dates_set_all_inactive(self) -> None:
        ids = [
            self.make_external_id(
                external_id="ABC123",
                id_active_from_datetime=datetime.datetime(2020, 1, 1),
                id_active_to_datetime=datetime.datetime(2020, 2, 2),
            ),
            # Pick this ID because it ended most recently
            self.make_external_id(
                external_id="DEF456",
                id_active_from_datetime=datetime.datetime(2020, 1, 1),
                id_active_to_datetime=datetime.datetime(2020, 3, 3),
            ),
            self.make_external_id(
                external_id="ZZZ999",
                id_active_from_datetime=datetime.datetime(2019, 1, 1),
                id_active_to_datetime=datetime.datetime(2020, 3, 3),
            ),
            self.make_external_id(
                external_id="LMN456",
                id_active_from_datetime=datetime.datetime(2019, 3, 3),
                id_active_to_datetime=datetime.datetime(2020, 2, 2),
            ),
        ]
        result = select_most_recently_active_person_external_id(ids)
        self.assertEqual(result.external_id, "DEF456")

    def test_raise_on_active_from_null_by_default(self) -> None:
        ids = [
            self.make_external_id(external_id="ABC123", id_active_from_datetime=None)
        ]
        with self.assertRaisesRegex(
            ValueError,
            r"Found null id_active_from_datetime value on external_id \["
            r"StatePersonExternalId\(external_id='ABC123', id_type='US_XX_ID_TYPE', "
            r"person_external_id_id=1\)\].",
        ):
            select_most_recently_active_person_external_id(ids)

    def test_all_dates_null(self) -> None:
        ids = [
            self.make_external_id(external_id="ABC123", id_active_from_datetime=None),
            self.make_external_id(external_id="DEF456", id_active_from_datetime=None),
            self.make_external_id(external_id="ZZZ999", id_active_from_datetime=None),
            self.make_external_id(external_id="LMN456", id_active_from_datetime=None),
        ]
        result = select_most_recently_active_person_external_id(
            ids, enforce_nonnull_id_active_from=False
        )
        self.assertEqual(result.external_id, "ZZZ999")

    def test_raises_on_empty_list(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Cannot call select_most_recently_active_person_external_id\(\) with an "
            r"empty external_ids list",
        ):
            select_most_recently_active_person_external_id([])

    def test_raises_on_multiple_id_types(self) -> None:
        ids = [
            self.make_external_id(
                external_id="ABC123",
                id_type=_ID_TYPE,
                id_active_from_datetime=datetime.datetime(2020, 1, 1),
            ),
            self.make_external_id(
                external_id="DEF456",
                id_type=_ID_TYPE_2,
                id_active_from_datetime=datetime.datetime(2020, 2, 2),
            ),
        ]
        with self.assertRaisesRegex(
            ValueError, r"Found multiple id_types in the provided external_ids list"
        ):
            _ = select_most_recently_active_person_external_id(ids)

    def test_raises_on_equal_external_ids(self) -> None:
        id1 = self.make_external_id(
            external_id="SAME123",
            id_active_from_datetime=datetime.datetime(2020, 1, 1),
        )
        id2 = self.make_external_id(
            external_id="SAME123",
            id_active_from_datetime=datetime.datetime(2020, 2, 2),
        )
        with self.assertRaisesRegex(
            ValueError,
            r"Found multiple external ids with external_id \[SAME123\] and id_type \[US_XX_ID_TYPE\]",
        ):
            _ = select_alphabetically_highest_person_external_id([id1, id2])


class TestSelectSingleExternalIdWithIsCurrentDisplayId(unittest.TestCase):
    """Unittests for the select_single_external_id_with_is_current_display_id() helper
    in normalize_external_ids_helpers.py"""

    def make_external_id(
        self,
        *,
        external_id: str,
        is_current_display_id_for_type: bool | None,
        id_type: str = _ID_TYPE
    ) -> StatePersonExternalId:
        return StatePersonExternalId(
            person_external_id_id=1,
            state_code=StateCode.US_XX.value,
            external_id=external_id,
            id_type=id_type,
            is_current_display_id_for_type=is_current_display_id_for_type,
            id_active_from_datetime=None,
            id_active_to_datetime=None,
        )

    def test_single(self) -> None:
        ids = [
            self.make_external_id(
                external_id="ABC123", is_current_display_id_for_type=True
            ),
        ]
        result = select_single_external_id_with_is_current_display_id(ids)
        self.assertEqual(result.external_id, "ABC123")

    def test_multiple(self) -> None:
        ids = [
            self.make_external_id(
                external_id="ABC123", is_current_display_id_for_type=False
            ),
            self.make_external_id(
                external_id="ZZZ999", is_current_display_id_for_type=False
            ),
            self.make_external_id(
                external_id="LMN456", is_current_display_id_for_type=True
            ),
        ]
        result = select_single_external_id_with_is_current_display_id(ids)
        self.assertEqual(result.external_id, "LMN456")

    def test_raises_on_no_is_current_set(self) -> None:
        ids = [
            self.make_external_id(
                external_id="ABC123", is_current_display_id_for_type=False
            ),
            self.make_external_id(
                external_id="ZZZ999", is_current_display_id_for_type=False
            ),
            self.make_external_id(
                external_id="LMN456", is_current_display_id_for_type=False
            ),
        ]
        with self.assertRaisesRegex(
            ValueError,
            r"Did not find any external_id with is_current_display_id_for_type=True.",
        ):
            _ = select_single_external_id_with_is_current_display_id(ids)

    def test_raises_on_multiple_is_current_set(self) -> None:
        ids = [
            self.make_external_id(
                external_id="ABC123", is_current_display_id_for_type=True
            ),
            self.make_external_id(
                external_id="ZZZ999", is_current_display_id_for_type=False
            ),
            self.make_external_id(
                external_id="LMN456", is_current_display_id_for_type=True
            ),
        ]
        with self.assertRaisesRegex(
            ValueError,
            r"Found more than one external_id with is_current_display_id_for_type=True",
        ):
            _ = select_single_external_id_with_is_current_display_id(ids)

    def test_raises_on_empty_list(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Cannot call select_single_external_id_with_is_current_display_id\(\) "
            r"with an empty external_ids list",
        ):
            select_single_external_id_with_is_current_display_id([])

    def test_raises_on_multiple_id_types(self) -> None:
        ids = [
            self.make_external_id(
                external_id="ABC123",
                id_type=_ID_TYPE,
                is_current_display_id_for_type=True,
            ),
            self.make_external_id(
                external_id="DEF456",
                id_type=_ID_TYPE_2,
                is_current_display_id_for_type=True,
            ),
        ]
        with self.assertRaisesRegex(
            ValueError, r"Found multiple id_types in the provided external_ids list"
        ):
            _ = select_single_external_id_with_is_current_display_id(ids)

    def test_raises_on_equal_external_ids(self) -> None:
        id1 = self.make_external_id(
            external_id="SAME123", is_current_display_id_for_type=True
        )
        id2 = self.make_external_id(
            external_id="SAME123", is_current_display_id_for_type=True
        )
        with self.assertRaisesRegex(
            ValueError,
            r"Found multiple external ids with external_id \[SAME123\] and id_type \[US_XX_ID_TYPE\]",
        ):
            _ = select_single_external_id_with_is_current_display_id([id1, id2])
