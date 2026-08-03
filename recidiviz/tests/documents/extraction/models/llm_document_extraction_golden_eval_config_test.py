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
"""Tests for LLMDocumentExtractionGoldenEvalConfig."""
import re
from typing import Any
from unittest import TestCase

from recidiviz.documents.extraction.models.llm_document_extraction_golden_eval_config import (
    GoldenEvalTestType,
    LLMDocumentExtractionGoldenEvalConfig,
)
from recidiviz.utils.yaml_dict import YAMLDict

_SHEET_URI = "https://docs.google.com/spreadsheets/d/1XVxpTA6ymYctzpB2G24V3KAcl8eozihFlA3OjfyjSU4"


def _parse(**body: Any) -> LLMDocumentExtractionGoldenEvalConfig:
    """Parses a `golden_eval` block, defaulting to a valid one that |body|
    overrides key by key.
    """
    contents: dict[str, Any] = {
        "source_sheet_uri": _SHEET_URI,
        "accuracy_thresholds": {"unit": 1.0, "sample": 0.9},
    }
    contents.update(body)
    return LLMDocumentExtractionGoldenEvalConfig.from_yaml_dict(YAMLDict(contents))


class LLMDocumentExtractionGoldenEvalConfigTest(TestCase):
    """Tests for LLMDocumentExtractionGoldenEvalConfig."""

    def test_parse(self) -> None:
        self.assertEqual(
            LLMDocumentExtractionGoldenEvalConfig(
                source_sheet_uri=_SHEET_URI,
                accuracy_thresholds={
                    GoldenEvalTestType.UNIT: 1.0,
                    GoldenEvalTestType.SAMPLE: 0.9,
                },
            ),
            _parse(),
        )

    def test_empty_source_sheet_uri_raises(self) -> None:
        with self.assertRaisesRegex(ValueError, r"String value should not be empty."):
            _parse(source_sheet_uri="")

    def test_missing_test_type_threshold_raises(self) -> None:
        with self.assertRaisesRegex(
            KeyError, re.escape("Expected nonnull [sample] in input")
        ):
            _parse(accuracy_thresholds={"unit": 1.0})

    def test_unknown_test_type_threshold_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Found unexpected golden eval accuracy thresholds for test "
                "types: {'integration'}"
            ),
        ):
            _parse(accuracy_thresholds={"unit": 1.0, "sample": 0.9, "integration": 0.5})

    def test_threshold_above_one_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Field \[accuracy_thresholds\] on "
            r"\[LLMDocumentExtractionGoldenEvalConfig\] must be between 0 and 1, "
            r"inclusive. Found value \[1.5\]",
        ):
            _parse(accuracy_thresholds={"unit": 1.5, "sample": 0.9})

    def test_threshold_below_zero_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Field \[accuracy_thresholds\] on "
            r"\[LLMDocumentExtractionGoldenEvalConfig\] must be between 0 and 1, "
            r"inclusive. Found value \[-0.1\]",
        ):
            _parse(accuracy_thresholds={"unit": -0.1, "sample": 0.9})

    def test_int_threshold_raises(self) -> None:
        # Thresholds must be authored as decimals so `1` can't be confused for a
        # count.
        with self.assertRaisesRegex(
            ValueError, re.escape("The field [unit] must be of type [<class 'float'>]")
        ):
            _parse(accuracy_thresholds={"unit": 1, "sample": 0.9})

    def test_direct_construction_requires_every_test_type(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Field [accuracy_thresholds] on "
                "[LLMDocumentExtractionGoldenEvalConfig] is missing a threshold "
                "for test types: ['sample']"
            ),
        ):
            LLMDocumentExtractionGoldenEvalConfig(
                source_sheet_uri=_SHEET_URI,
                accuracy_thresholds={GoldenEvalTestType.UNIT: 1.0},
            )
