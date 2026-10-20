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
where the human-labeled eval set of each extraction stage lives, and how
accurate each test type has to be at that stage.
"""
from enum import StrEnum
from typing import Any

import attr

from recidiviz.common import attr_validators
from recidiviz.documents.extraction.llm_extraction_view_graph_stage import (
    LLMExtractionViewGraphStage,
)
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


def _declares_first_order_stage(
    instance: Any,
    attribute: attr.Attribute,
    value: dict[
        LLMExtractionViewGraphStage, "LLMDocumentExtractionGoldenEvalStageConfig"
    ],
) -> None:
    """Validates that a FIRST_ORDER stage config is declared."""
    if LLMExtractionViewGraphStage.FIRST_ORDER not in value:
        raise ValueError(
            f"Field [{attribute.name}] on [{type(instance).__name__}] has no "
            f"[{LLMExtractionViewGraphStage.FIRST_ORDER.value}] entry. Add a "
            f"golden_eval.{LLMExtractionViewGraphStage.FIRST_ORDER.value} block to the "
            f"state extractor's extractor.yaml."
        )


@attr.define(frozen=True, kw_only=True)
class LLMDocumentExtractionGoldenEvalStageConfig:
    """The parsed golden eval block for one extraction stage of a state extractor."""

    source_sheet_uri: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """URI of the Google Sheet tab holding this stage's eval set."""

    accuracy_thresholds: dict[GoldenEvalTestType, float] = attr.ib(
        validator=[
            attr_validators.is_dict_where_each(
                key_validator=attr.validators.in_(GoldenEvalTestType),
                value_validator=attr_validators.is_float_between_zero_and_one,
            ),
            _covers_every_test_type,
        ]
    )
    """Minimum accuracy each test type must reach on this stage's eval set, as a
    proportion in [0, 1].
    """

    @classmethod
    def from_yaml_dict(
        cls, yaml_dict: YAMLDict
    ) -> "LLMDocumentExtractionGoldenEvalStageConfig":
        """Returns the stage config parsed from one stage block of a state
        extractor's golden_eval block. Thresholds must be decimals (1.0, not 1).
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
                f"Found unexpected config values in golden_eval stage block: "
                f"{repr(yaml_dict.get())}"
            )

        return cls(
            source_sheet_uri=source_sheet_uri,
            accuracy_thresholds=accuracy_thresholds,
        )


@attr.define(frozen=True, kw_only=True)
class LLMDocumentExtractionGoldenEvalConfig:
    """The parsed golden_eval block of a state extractor's extractor.yaml."""

    stage_configs: dict[
        LLMExtractionViewGraphStage, LLMDocumentExtractionGoldenEvalStageConfig
    ] = attr.ib(
        validator=[
            attr_validators.is_dict_where_each(
                key_validator=attr.validators.in_(LLMExtractionViewGraphStage),
                value_validator=attr.validators.instance_of(
                    LLMDocumentExtractionGoldenEvalStageConfig
                ),
            ),
            _declares_first_order_stage,
        ]
    )
    """The golden eval config of each extraction stage that has an eval set. The
    FIRST_ORDER stage is always present.
    """

    def config_for_stage(
        self, stage: LLMExtractionViewGraphStage
    ) -> LLMDocumentExtractionGoldenEvalStageConfig:
        """Returns the golden eval config for the given stage, raising when the
        extractor declares none for it.
        """
        if stage not in self.stage_configs:
            raise ValueError(
                f"No golden_eval.{stage.value} block is declared, so there is no "
                f"[{stage.value}] eval set to read. Add one to the state "
                f"extractor's extractor.yaml."
            )
        return self.stage_configs[stage]

    @classmethod
    def from_yaml_dict(
        cls, yaml_dict: YAMLDict
    ) -> "LLMDocumentExtractionGoldenEvalConfig":
        """Returns the golden eval config parsed from a state extractor's
        golden_eval block, which holds one block per stage keyed by stage name.
        """
        stage_configs = {
            stage: LLMDocumentExtractionGoldenEvalStageConfig.from_yaml_dict(stage_dict)
            for stage in LLMExtractionViewGraphStage
            if (stage_dict := yaml_dict.pop_dict_optional(stage.value)) is not None
        }

        if yaml_dict:
            raise ValueError(
                f"Found unexpected config values in golden_eval block: "
                f"{repr(yaml_dict.get())}. Expected only stage blocks keyed by "
                f"{[stage.value for stage in LLMExtractionViewGraphStage]}."
            )

        return cls(stage_configs=stage_configs)
