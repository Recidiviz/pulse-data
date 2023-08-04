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
"""Tests for the RootEntityUpdateMerger class."""
import unittest

import attr

from recidiviz.persistence.entity.entity_utils import CoreEntityFieldIndex
from recidiviz.persistence.entity_matching.root_entity_update_merger import (
    RootEntityUpdateMerger,
)
from recidiviz.tests.persistence.entity_matching.us_xx_entity_builders import (
    make_person,
    make_person_external_id,
    make_staff,
    make_staff_external_id,
)


class TestRootEntityUpdateMerger(unittest.TestCase):
    """Tests for the RootEntityUpdateMerger class."""

    def setUp(self) -> None:
        self.merger = RootEntityUpdateMerger(CoreEntityFieldIndex())

    def test_merge_people_exact_match(self) -> None:
        previous_root_entity = make_person(
            external_ids=[
                make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1")
            ],
        )
        entity_updates = make_person(
            external_ids=[
                make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1")
            ],
        )

        # Expect no change since there is no new info in the updates
        expected_result = attr.evolve(previous_root_entity)

        result = self.merger.merge_root_entity_trees(
            old_root_entity=previous_root_entity,
            root_entity_updates=entity_updates,
        )

        self.assertEqual(expected_result, result)

    def test_merge_staff_exact_match(self) -> None:
        previous_root_entity = make_staff(
            external_ids=[
                make_staff_external_id(external_id="ID_1", id_type="ID_TYPE_1")
            ],
        )
        entity_updates = make_staff(
            external_ids=[
                make_staff_external_id(external_id="ID_1", id_type="ID_TYPE_1")
            ],
        )

        # Expect no change since there is no new info in the updates
        expected_result = attr.evolve(previous_root_entity)

        result = self.merger.merge_root_entity_trees(
            old_root_entity=previous_root_entity,
            root_entity_updates=entity_updates,
        )

        self.assertEqual(expected_result, result)

    def test_merge_tree_no_match(self) -> None:
        # Test no match same type different id
        with self.assertRaisesRegex(
            ValueError,
            (
                r"Attempting to merge updates for root entity with external ids "
                r"\[StateStaffExternalId\(external_id='ID_2', state_code='US_XX', "
                r"id_type='ID_TYPE_1'.*\)\] into entity with non-overlapping external "
                r"ids \[StateStaffExternalId\(external_id='ID_1', state_code='US_XX', "
                r"id_type='ID_TYPE_1'.*\)\]."
            ),
        ):
            _ = self.merger.merge_root_entity_trees(
                old_root_entity=make_staff(
                    external_ids=[
                        make_staff_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                    ],
                ),
                root_entity_updates=make_staff(
                    external_ids=[
                        make_staff_external_id(external_id="ID_2", id_type="ID_TYPE_1")
                    ],
                ),
            )

        # Test no match same id different type
        with self.assertRaisesRegex(
            ValueError,
            (
                r"Attempting to merge updates for root entity with external ids "
                r"\[StateStaffExternalId\(external_id='ID_1', state_code='US_XX', "
                r"id_type='ID_TYPE_2'.*\)\] into entity with non-overlapping external "
                r"ids \[StateStaffExternalId\(external_id='ID_1', state_code='US_XX', "
                r"id_type='ID_TYPE_1'.*\)\]."
            ),
        ):
            _ = self.merger.merge_root_entity_trees(
                old_root_entity=make_staff(
                    external_ids=[
                        make_staff_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                    ],
                ),
                root_entity_updates=make_staff(
                    external_ids=[
                        make_staff_external_id(external_id="ID_1", id_type="ID_TYPE_2")
                    ],
                ),
            )
