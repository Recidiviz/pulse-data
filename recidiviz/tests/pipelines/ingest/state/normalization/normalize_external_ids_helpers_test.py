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
import unittest

from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import StatePersonExternalId
from recidiviz.pipelines.ingest.state.normalization.normalize_external_ids_helpers import (
    select_alphabetically_highest_person_external_id,
)

_ID_TYPE = "US_XX_ID_TYPE"
_ID_TYPE_2 = "US_XX_ID_TYPE_2"


class TestNormalizeExternalIdsHelpers(unittest.TestCase):
    """Unittests for normalize_external_ids_helpers.py"""

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

    def test_returns_highest_alphabetical_id_single(self) -> None:
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

    def test_equal_external_ids_returns_first_in_list(self) -> None:
        id1 = self.make_external_id(external_id="SAME123")
        id2 = self.make_external_id(external_id="SAME123")
        with self.assertRaisesRegex(
            ValueError,
            r"Found multiple external ids with external_id \[SAME123\] and id_type \[US_XX_ID_TYPE\]",
        ):
            _ = select_alphabetically_highest_person_external_id([id1, id2])
