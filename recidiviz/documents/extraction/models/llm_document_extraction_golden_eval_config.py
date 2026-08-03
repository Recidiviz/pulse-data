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
"""The golden eval configuration for a single (collection, state) LLM extractor:
where that extractor's human-labeled eval set lives, and how accurate each test
type has to be.
"""
from enum import StrEnum
from typing import Any

import attr

from recidiviz.common import attr_validators
from recidiviz.utils.yaml_dict import YAMLDict


class GoldenEvalTestType(StrEnum):
    """The types of golden eval tests, each with its own accuracy threshold."""

    UNIT = "unit"
    """Minimal, targeted examples exercising one behavior each."""

    SAMPLE = "sample"
    """Realistic full documents."""


def _covers_every_test_type(
    instance: Any, attribute: attr.Attribute, value: dict[GoldenEvalTestType, float]
) -> None:
    """Validates that a threshold is declared for every GoldenEvalTestType, so no
    test type is silently unscored.
    """
    if missing_test_types := set(GoldenEvalTestType) - set(value):
        raise ValueError(
            f"Field [{attribute.name}] on [{type(instance).__name__}] is missing a "
            f"threshold for test types: "
            f"{sorted(t.value for t in missing_test_types)}."
        )


@attr.define(frozen=True, kw_only=True)
class LLMDocumentExtractionGoldenEvalConfig:
    """The parsed `golden_eval` block of a state extractor's `extractor.yaml`."""

    source_sheet_uri: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """URI of the Google Sheet holding this extractor's eval set. Declared per
    (collection, state) extractor rather than once per collection, so adding a
    new state's extractor forces a conscious decision about its eval set; states
    that run the same collection may point at the same spreadsheet (optionally a
    distinct sub-sheet within it) or at entirely separate ones.
    """

    accuracy_thresholds: dict[GoldenEvalTestType, float] = attr.ib(
        validator=[
            attr_validators.is_dict_where_each(
                key_validator=attr.validators.in_(GoldenEvalTestType),
                value_validator=attr_validators.is_float_between_zero_and_one,
            ),
            _covers_every_test_type,
        ]
    )
    """Minimum accuracy each test type must reach, as a proportion in [0, 1]. Read
    by eval reporting and, once the CI gate blocks, by the gate itself.
    """

    @classmethod
    def from_yaml_dict(
        cls, yaml_dict: YAMLDict
    ) -> "LLMDocumentExtractionGoldenEvalConfig":
        """Returns the golden eval config parsed from a state extractor's
        `golden_eval` block. Thresholds must be decimals (`1.0`, not `1`).
        """
        source_sheet_uri = yaml_dict.pop("source_sheet_uri", str)

        thresholds_dict = yaml_dict.pop_dict("accuracy_thresholds")
        accuracy_thresholds = {
            test_type: thresholds_dict.pop(test_type.value, float)
            for test_type in GoldenEvalTestType
        }
        if thresholds_dict:
            raise ValueError(
                f"Found unexpected golden eval accuracy thresholds for test "
                f"types: {set(thresholds_dict.get())}."
            )

        if yaml_dict:
            raise ValueError(
                f"Found unexpected config values in golden_eval block: "
                f"{repr(yaml_dict.get())}"
            )

        return cls(
            source_sheet_uri=source_sheet_uri,
            accuracy_thresholds=accuracy_thresholds,
        )
