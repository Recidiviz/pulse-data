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
"""Tests for acronym_reference_data_entry.py."""
from unittest import TestCase

from recidiviz.documents.extraction.models.reference_data.acronym_reference_data_entry import (
    AcronymReferenceDataEntry,
)
from recidiviz.utils.yaml_dict import YAMLDict


class AcronymReferenceDataEntryTest(TestCase):
    """Tests for AcronymReferenceDataEntry."""

    def test_from_yaml_dict(self) -> None:
        self.assertEqual(
            AcronymReferenceDataEntry(acronym="PO", expansion="Parole Officer"),
            AcronymReferenceDataEntry.from_yaml_dict(
                YAMLDict({"acronym": "PO", "expansion": "Parole Officer"})
            ),
        )

    def test_dedup_key(self) -> None:
        entry = AcronymReferenceDataEntry(acronym="PO", expansion="Parole Officer")
        self.assertEqual("PO", entry.dedup_key)

    def test_parse_empty_acronym_raises(self) -> None:
        with self.assertRaisesRegex(ValueError, "String value should not be empty."):
            AcronymReferenceDataEntry.from_yaml_dict(
                YAMLDict({"acronym": "", "expansion": "Parole Officer"})
            )

    def test_parse_empty_expansion_raises(self) -> None:
        with self.assertRaisesRegex(ValueError, "String value should not be empty."):
            AcronymReferenceDataEntry.from_yaml_dict(
                YAMLDict({"acronym": "PO", "expansion": ""})
            )

    def test_parse_unexpected_key_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Found unexpected config values for acronym \[PO\]:",
        ):
            AcronymReferenceDataEntry.from_yaml_dict(
                YAMLDict({"acronym": "PO", "expansion": "Parole Officer", "extra": "x"})
            )
