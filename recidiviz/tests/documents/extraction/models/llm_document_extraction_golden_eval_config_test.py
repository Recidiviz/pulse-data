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

from recidiviz.documents.extraction.llm_extraction_view_graph_stage import (
    LLMExtractionViewGraphStage,
)
from recidiviz.documents.extraction.models.llm_document_extraction_golden_eval_config import (
    GoldenEvalTestType,
    LLMDocumentExtractionGoldenEvalConfig,
    LLMDocumentExtractionGoldenEvalStageConfig,
)
from recidiviz.utils.yaml_dict import YAMLDict

_SHEET_URI = "https://docs.google.com/spreadsheets/d/1XVxpTA6ymYctzpB2G24V3KAcl8eozihFlA3OjfyjSU4"
_ER_SHEET_URI = f"{_SHEET_URI}#gid=2"
_THRESHOLDS = {GoldenEvalTestType.UNIT: 1.0, GoldenEvalTestType.SAMPLE: 0.9}
_FIRST_ORDER = LLMExtractionViewGraphStage.FIRST_ORDER
_ENTITY_RESOLUTION = LLMExtractionViewGraphStage.ENTITY_RESOLUTION


def _stage_body(source_sheet_uri: str) -> dict[str, Any]:
    return {
        "source_sheet_uri": source_sheet_uri,
        "accuracy_thresholds": {"unit": 1.0, "sample": 0.9},
    }


def _stage_config(source_sheet_uri: str) -> LLMDocumentExtractionGoldenEvalStageConfig:
    return LLMDocumentExtractionGoldenEvalStageConfig(
        source_sheet_uri=source_sheet_uri, accuracy_thresholds=_THRESHOLDS
    )


def _parse(**body: Any) -> LLMDocumentExtractionGoldenEvalConfig:
    """Parses a golden_eval block, defaulting to a valid one with only a
    FIRST_ORDER block that |body| overrides key by key.
    """
    contents: dict[str, Any] = {"first_order": _stage_body(_SHEET_URI)}
    contents.update(body)
    return LLMDocumentExtractionGoldenEvalConfig.from_yaml_dict(YAMLDict(contents))


class LLMDocumentExtractionGoldenEvalConfigTest(TestCase):
    """Tests for LLMDocumentExtractionGoldenEvalConfig."""

    def test_parse_first_order_only(self) -> None:
        self.assertEqual(
            LLMDocumentExtractionGoldenEvalConfig(
                stage_configs={_FIRST_ORDER: _stage_config(_SHEET_URI)}
            ),
            _parse(),
        )

    def test_parse_both_stages(self) -> None:
        self.assertEqual(
            LLMDocumentExtractionGoldenEvalConfig(
                stage_configs={
                    _FIRST_ORDER: _stage_config(_SHEET_URI),
                    _ENTITY_RESOLUTION: _stage_config(_ER_SHEET_URI),
                }
            ),
            _parse(entity_resolution=_stage_body(_ER_SHEET_URI)),
        )

    def test_unknown_key_under_golden_eval_raises(self) -> None:
        with self.assertRaisesRegex(ValueError, r"unexpected.*not_a_stage"):
            _parse(not_a_stage=_stage_body(_SHEET_URI))

    def test_unknown_key_under_stage_block_raises(self) -> None:
        with self.assertRaisesRegex(ValueError, r"unexpected.*not_a_field"):
            _parse(first_order={**_stage_body(_SHEET_URI), "not_a_field": "x"})

    def test_missing_first_order_raises(self) -> None:
        with self.assertRaisesRegex(ValueError, r"first_order"):
            LLMDocumentExtractionGoldenEvalConfig.from_yaml_dict(
                YAMLDict({"entity_resolution": _stage_body(_ER_SHEET_URI)})
            )

    def test_unknown_test_type_threshold_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"unexpected golden eval accuracy thresholds.*integration"
        ):
            _parse(
                first_order={
                    "source_sheet_uri": _SHEET_URI,
                    "accuracy_thresholds": {
                        "unit": 1.0,
                        "sample": 0.9,
                        "integration": 0.5,
                    },
                }
            )

    def test_stage_config_direct_construction_requires_every_test_type(self) -> None:
        with self.assertRaisesRegex(
            ValueError, re.escape("missing a threshold for test types: ['sample']")
        ):
            LLMDocumentExtractionGoldenEvalStageConfig(
                source_sheet_uri=_SHEET_URI,
                accuracy_thresholds={GoldenEvalTestType.UNIT: 1.0},
            )

    def test_config_for_stage(self) -> None:
        config = _parse(entity_resolution=_stage_body(_ER_SHEET_URI))

        self.assertEqual(
            _stage_config(_SHEET_URI), config.config_for_stage(_FIRST_ORDER)
        )
        self.assertEqual(
            _stage_config(_ER_SHEET_URI), config.config_for_stage(_ENTITY_RESOLUTION)
        )

    def test_config_for_stage_without_entity_resolution_block_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"No golden_eval\.entity_resolution block is declared"
        ):
            _parse().config_for_stage(_ENTITY_RESOLUTION)
