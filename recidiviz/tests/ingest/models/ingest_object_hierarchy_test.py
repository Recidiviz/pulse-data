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

"""Tests for ingest_object_hierarchy"""

import unittest
from typing import Sequence

from recidiviz.ingest.models.ingest_object_hierarchy import get_ancestor_class_sequence


class FieldsDontMatchError(Exception):
    pass


class TestIngestObjectHierarchy(unittest.TestCase):
    """Tests for ingest_object_hierarchy."""

    def test_get_sequence_for_class_persons(self) -> None:
        actual = get_ancestor_class_sequence("person")
        expected = ()
        self.assertEqual(expected, actual)

        actual = get_ancestor_class_sequence("state_person")
        expected = ()
        self.assertEqual(expected, actual)

    def test_get_sequence_for_class_single_parent(self) -> None:
        actual = get_ancestor_class_sequence("bond")
        expected: Sequence[str] = ("person", "booking", "charge")
        self.assertEqual(expected, actual)

        actual = get_ancestor_class_sequence("state_supervision_sentence")
        expected = ("state_person", "state_sentence_group")
        self.assertEqual(expected, actual)

    def test_get_sequence_for_class_multiple_parents_enforced(self) -> None:
        actual = get_ancestor_class_sequence(
            "state_supervision_period",
            enforced_ancestor_choices={
                "state_sentence": "state_incarceration_sentence"
            },
        )

        expected = (
            "state_person",
            "state_sentence_group",
            "state_incarceration_sentence",
        )
        self.assertEqual(expected, actual)

    def test_get_sequence_for_class_multiple_parents_enforced_bad_key(self) -> None:
        with self.assertRaises(ValueError):
            get_ancestor_class_sequence(
                "state_supervision_period",
                enforced_ancestor_choices={"nonsense": "whatever"},
            )

    def test_get_sequence_for_class_multiple_parents_enforced_bad_choice(self) -> None:
        with self.assertRaises(ValueError):
            get_ancestor_class_sequence(
                "state_supervision_period",
                enforced_ancestor_choices={"state_sentence": "bogus"},
            )

    def test_get_sequence_for_class_multiple_parents_enforced_over_chain(self) -> None:
        actual = get_ancestor_class_sequence(
            "state_supervision_period",
            ancestor_chain={"state_person": "12345"},
            enforced_ancestor_choices={"state_sentence": "state_supervision_sentence"},
        )

        expected = (
            "state_person",
            "state_sentence_group",
            "state_supervision_sentence",
        )
        self.assertEqual(expected, actual)

    def test_get_sequence_for_class_multiple_parents_chain(self) -> None:
        actual = get_ancestor_class_sequence(
            "state_incarceration_period",
            ancestor_chain={
                "state_person": "12345",
                "state_incarceration_sentence": "45678",
            },
        )

        expected = (
            "state_person",
            "state_sentence_group",
            "state_incarceration_sentence",
        )
        self.assertEqual(expected, actual)

    def test_get_sequence_for_class_multiple_parents_further_downstream(self) -> None:
        actual = get_ancestor_class_sequence(
            "state_supervision_violation_response",
            enforced_ancestor_choices={"state_sentence": "state_supervision_sentence"},
        )

        expected = (
            "state_person",
            "state_sentence_group",
            "state_supervision_sentence",
            "state_supervision_period",
            "state_supervision_violation",
        )
        self.assertEqual(expected, actual)
