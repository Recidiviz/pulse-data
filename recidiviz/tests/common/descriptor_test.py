# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for descriptor.py."""
from unittest import TestCase

from recidiviz.common.descriptor import Descriptor
from recidiviz.utils.yaml_dict import YAMLDict


class DescriptorTest(TestCase):
    """Tests for the Descriptor value object."""

    def test_indefinite_uses_an_before_vowel(self) -> None:
        self.assertEqual(
            "an employer",
            Descriptor(singular="employer", plural="employers").indefinite,
        )

    def test_indefinite_uses_a_before_consonant_multiword(self) -> None:
        # The article attaches to the whole multi-word singular.
        self.assertEqual(
            "a place of residence",
            Descriptor(
                singular="place of residence", plural="places of residence"
            ).indefinite,
        )

    def test_indefinite_article_chosen_case_insensitively(self) -> None:
        # The leading letter is lowercased to pick "a"/"an", but the noun's own
        # casing is preserved in the output.
        self.assertEqual(
            "an Organization",
            Descriptor(singular="Organization", plural="Organizations").indefinite,
        )

    def test_indefinite_uses_naive_letter_heuristic(self) -> None:
        # The article is chosen purely from the leading letter, so a word whose
        # spoken form disagrees with its spelling (here, the silent "h") gets the
        # "wrong" article. This is a documented, accepted limitation — our real
        # descriptors do not hit these cases.
        self.assertEqual(
            "a hour", Descriptor(singular="hour", plural="hours").indefinite
        )

    def test_empty_singular_raises(self) -> None:
        with self.assertRaisesRegex(ValueError, "String value should not be empty"):
            Descriptor(singular="", plural="things")

    def test_empty_plural_raises(self) -> None:
        with self.assertRaisesRegex(ValueError, "String value should not be empty"):
            Descriptor(singular="thing", plural="")

    def test_from_yaml_dict(self) -> None:
        self.assertEqual(
            Descriptor(singular="employer", plural="employers"),
            Descriptor.from_yaml_dict(
                YAMLDict({"singular": "employer", "plural": "employers"})
            ),
        )

    def test_from_yaml_dict_unexpected_key_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "Found unexpected config values in descriptor: "
        ):
            Descriptor.from_yaml_dict(
                YAMLDict({"singular": "employer", "plural": "employers", "bogus": "x"})
            )

    def test_from_yaml_dict_missing_key_raises(self) -> None:
        with self.assertRaisesRegex(KeyError, r"Expected nonnull \[plural\]"):
            Descriptor.from_yaml_dict(YAMLDict({"singular": "employer"}))

    def test_from_yaml_dict_wrong_type_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"The field \[singular\] must be of type"
        ):
            Descriptor.from_yaml_dict(YAMLDict({"singular": 5, "plural": "employers"}))
