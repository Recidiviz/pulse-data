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
"""Tests for llm_extractor_output_format_instructions_generator.py.

`test_generate_matches_golden` pins the fully assembled `{output_instructions}`
block against an inline expected string; the remaining tests exercise each
composable slot renderer directly.
"""
from typing import Any
from unittest import TestCase

from recidiviz.documents.extraction.models.llm_request_output_schema import (
    LLMRequestOutputSchema,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field import (
    ConfidenceLevel,
    NullReason,
)
from recidiviz.documents.extraction.prompt_generation.llm_extractor_output_format_instructions_generator import (
    LLMExtractorOutputFormatInstructionsGenerator,
)
from recidiviz.utils.yaml_dict import YAMLDict

_DESCRIPTION = "A description that is long enough to be meaningful."


def _field(name: str, *, field_type: str = "STRING", **extra: Any) -> dict[str, Any]:
    """Returns a raw field-definition dict for use as one entry of a scope."""
    return {"name": name, "type": field_type, "description": _DESCRIPTION, **extra}


def _enum_values(*names: str) -> list[dict[str, str]]:
    """Returns raw `{name, description}` enum value dicts, each description unique
    per name (a field's value descriptions must be distinct).
    """
    return [{"name": name, "description": f"{_DESCRIPTION} ({name})"} for name in names]


def _build_schema(
    *user_fields: dict[str, Any],
    relevance_criteria: str = "Whether the document mentions x",
) -> LLMRequestOutputSchema:
    return LLMRequestOutputSchema.from_yaml_dict(
        yaml_dict=YAMLDict(
            {
                "full_batch_description": _DESCRIPTION,
                "result_level_description": _DESCRIPTION,
                "inferred_fields": list(user_fields),
            }
        ),
        relevance_criteria=relevance_criteria,
        default_minimum_confidence_level=ConfidenceLevel.INFERRED,
    )


# The exact block produced for the schema built in `test_generate_matches_golden`.
# Regenerate by running that schema through `.generate(...)` if the template or a
# slot renderer intentionally changes.
_EXPECTED_GENERATE_OUTPUT = """\
CRITICAL INSTRUCTION: First determine whether this document is relevant. A document is
considered relevant according to the following criteria:
Whether the document mentions jobs, work, pay, or employers
If it is not relevant, set is_relevant to false and use null_reason='no_info_found'
for all other fields.

Output fields:
- is_relevant (boolean, bare value): Whether the document mentions jobs, work, pay, or employers
- summary (string, bare value): A description that is long enough to be meaningful.
- primary_status (enum): A description that is long enough to be meaningful. Allowed values:
  - employed: A description that is long enough to be meaningful. (employed)
  - unemployed: A description that is long enough to be meaningful. (unemployed)
- employers (list): A description that is long enough to be meaningful.
  - employer_name (string): A description that is long enough to be meaningful.
  - pay_rate_amount (number): A description that is long enough to be meaningful.
  - pay_rate_time_period (enum): A description that is long enough to be meaningful. Allowed values:
    - hour: A description that is long enough to be meaningful. (hour)
    - year: A description that is long enough to be meaningful. (year)

Fields must be logically consistent:
- `employers` applies only when `primary_status` is one of ['employed']; otherwise leave it empty.
- `employers` must have at least one entry when `primary_status` is one of ['employed'].
- `employers[].pay_rate_time_period` applies only when `pay_rate_amount` is set; otherwise set it to null with null_reason='not_applicable'.

Each document produces exactly one extraction result object, conforming
to the output schema supplied separately with this request. Each field
listed above is reported with a metadata wrapper, in one of two shapes
(the schema enforces exactly one). Emit only the keys shown for the
shape you use — the key that distinguishes the other shape
(`value` or `null_reason`) is absent, not
null. Within a shape, include every key shown. Exceptions:
- fields tagged "bare value", which you output directly
- "list" fields, which are arrays whose elements are objects (each sub-field follows these same rules)

When you extract a value:
  {
    "adversarial_interpretation": <The strongest alternative reading of the source text — how one could reasonably arrive at a different value, or why a value might be present when none was extracted. Null when no reasonable alternative exists.>,
    "value": <the extracted value>,
    "confidence_level": "speculative" | "inferred" | "explicit" | "verbatim",
    "citations": [
      {
        "text": <The exact quoted text from the document.>,
        "start": <Character offset in the document where the quoted text begins.>,
        "end": <Character offset in the document where the quoted text ends.>,
      },
      ...
    ],  // Exact quotes from the document supporting the extracted value; at least one is required.
  }

When you cannot extract a value:
  {
    "adversarial_interpretation": <The strongest alternative reading of the source text — how one could reasonably arrive at a different value, or why a value might be present when none was extracted. Null when no reasonable alternative exists.>,
    "null_reason": "not_applicable" | "no_info_found" | "explicitly_unknown",
    "confidence_level": "speculative" | "inferred" | "explicit" | "verbatim",
    "citations": [
      {
        "text": <The exact quoted text from the document.>,
        "start": <Character offset in the document where the quoted text begins.>,
        "end": <Character offset in the document where the quoted text ends.>,
      },
      ...
    ],  // Exact quotes from the document supporting why no value was extracted. Required, but may be empty — include a quote only when one shows the absence (e.g. for explicitly_unknown).
  }

- When `adversarial_interpretation` is non-null, set
  `confidence_level` to "speculative".

`confidence_level` values, ordered from least to most
confident:
- "speculative": Lowest confidence: a plausible alternative reading of the document exists (i.e. the model recorded an adversarial interpretation), so there is genuine doubt about whether the extracted value is correct.
- "inferred": The value follows from one clear inferential step over direct evidence in the document; there is no adversarial interpretation.
- "explicit": The value is directly stated in the document, with at most minor normalization (synonym, abbreviation, or paraphrase).
- "verbatim": Highest confidence: the value appears exactly as-is in the document — the citation text IS the value, with zero rewording or substitution.

`null_reason` values, used only on the null shape above:
- "not_applicable": The field does not apply given the values of the other fields it depends on — for example, a field that is only meaningful for a particular value of another field, when that value does not hold.
- "no_info_found": The document does not mention this information.
- "explicitly_unknown": The document acknowledges the information but states it is unknown."""


class GenerateTest(TestCase):
    """The fully assembled `{output_instructions}` block."""

    def test_generate_matches_golden(self) -> None:
        schema = _build_schema(
            _field("summary", field_mode="STRUCTURAL"),
            _field(
                "primary_status",
                field_type="ENUM",
                values=_enum_values("employed", "unemployed"),
                required=True,
            ),
            _field(
                "employers",
                field_type="ARRAY_OF_STRUCT",
                applicable_when_value={"primary_status": ["employed"]},
                required_when_value={"primary_status": ["employed"]},
                primary_keys=["employer_name"],
                fields=[
                    _field("employer_name", required=True),
                    _field("pay_rate_amount", field_type="FLOAT"),
                    _field(
                        "pay_rate_time_period",
                        field_type="ENUM",
                        values=_enum_values("hour", "year"),
                        applicable_when_nonnull="pay_rate_amount",
                    ),
                ],
            ),
            relevance_criteria="Whether the document mentions jobs, work, pay, or employers",
        )
        self.assertEqual(
            _EXPECTED_GENERATE_OUTPUT,
            LLMExtractorOutputFormatInstructionsGenerator.generate(schema),
        )


class OutputFieldsDescriptionTest(TestCase):
    """`render_output_fields_description`."""

    def test_lists_all_fields_with_types_and_nested_subfields(self) -> None:
        schema = _build_schema(
            _field("summary", field_mode="STRUCTURAL"),
            _field(
                "status",
                field_type="ENUM",
                values=_enum_values("employed", "unemployed"),
            ),
            _field(
                "employers",
                field_type="ARRAY_OF_STRUCT",
                primary_keys=["employer_name"],
                fields=[
                    _field("employer_name"),
                    _field("pay_rate_amount", field_type="FLOAT"),
                ],
            ),
            relevance_criteria="Whether the document mentions employment",
        )
        expected = (
            "- is_relevant (boolean, bare value): Whether the document mentions employment\n"
            "- summary (string, bare value): A description that is long enough to be meaningful.\n"
            "- status (enum): A description that is long enough to be meaningful. Allowed values:\n"
            "  - employed: A description that is long enough to be meaningful. (employed)\n"
            "  - unemployed: A description that is long enough to be meaningful. (unemployed)\n"
            "- employers (list): A description that is long enough to be meaningful.\n"
            "  - employer_name (string): A description that is long enough to be meaningful.\n"
            "  - pay_rate_amount (number): A description that is long enough to be meaningful."
        )
        self.assertEqual(
            expected,
            LLMExtractorOutputFormatInstructionsGenerator.render_output_fields_description(
                schema
            ),
        )


class SemanticConsistencyConstraintsTest(TestCase):
    """`render_semantic_consistency_constraints`."""

    def test_renders_all_constraint_types_including_nested(self) -> None:
        schema = _build_schema(
            _field(
                "status",
                field_type="ENUM",
                values=_enum_values("employed", "unemployed"),
                required=True,
            ),
            _field("anchor"),
            _field(
                "a_applicable_value", applicable_when_value={"status": ["employed"]}
            ),
            _field(
                "a_not_applicable_value",
                not_applicable_when_value={"status": ["unemployed"]},
            ),
            _field("a_applicable_nonnull", applicable_when_nonnull="anchor"),
            _field("a_required_nonnull", required_when_nonnull="anchor"),
            _field("a_required_value", required_when_value={"status": ["employed"]}),
            _field(
                "employers",
                field_type="ARRAY_OF_STRUCT",
                primary_keys=["employer_name"],
                fields=[
                    _field("employer_name", required=True),
                    _field("pay_rate_amount", field_type="FLOAT"),
                    _field(
                        "pay_rate_time_period",
                        field_type="ENUM",
                        values=_enum_values("hour", "year"),
                        required_when_nonnull="pay_rate_amount",
                    ),
                ],
            ),
            relevance_criteria="Whether the document mentions employment",
        )
        expected = (
            "Fields must be logically consistent:\n"
            "- `a_applicable_value` applies only when `status` is one of ['employed']; "
            "otherwise set it to null with null_reason='not_applicable'.\n"
            "- `a_not_applicable_value` must be null with null_reason='not_applicable' "
            "when `status` is one of ['unemployed'].\n"
            "- `a_applicable_nonnull` applies only when `anchor` is set; otherwise set "
            "it to null with null_reason='not_applicable'.\n"
            "- `a_required_nonnull` must be set when `anchor` is set.\n"
            "- `a_required_value` must be set when `status` is one of ['employed'].\n"
            "- `employers[].pay_rate_time_period` must be set when `pay_rate_amount` is set."
        )
        self.assertEqual(
            expected,
            LLMExtractorOutputFormatInstructionsGenerator.render_semantic_consistency_constraints(
                schema
            ),
        )

    def test_list_field_constraint_wording(self) -> None:
        # An ARRAY_OF_STRUCT field uses "leave it empty" / "must have at least one
        # entry" rather than the scalar null_reason wording.
        schema = _build_schema(
            _field(
                "status",
                field_type="ENUM",
                values=_enum_values("employed", "unemployed"),
                required=True,
            ),
            _field(
                "employers",
                field_type="ARRAY_OF_STRUCT",
                applicable_when_value={"status": ["employed"]},
                required_when_value={"status": ["employed"]},
                primary_keys=["employer_name"],
                fields=[_field("employer_name", required=True)],
            ),
            _field(
                "certifications",
                field_type="ARRAY_OF_STRUCT",
                required_when_nonnull="status",
                primary_keys=["certification_name"],
                fields=[_field("certification_name", required=True)],
            ),
            relevance_criteria="Whether the document mentions employment",
        )
        expected = (
            "Fields must be logically consistent:\n"
            "- `employers` applies only when `status` is one of ['employed']; "
            "otherwise leave it empty.\n"
            "- `employers` must have at least one entry when `status` is one of "
            "['employed'].\n"
            "- `certifications` must have at least one entry when `status` is set."
        )
        self.assertEqual(
            expected,
            LLMExtractorOutputFormatInstructionsGenerator.render_semantic_consistency_constraints(
                schema
            ),
        )

    def test_no_constraints_renders_empty(self) -> None:
        schema = _build_schema(_field("note"))
        self.assertEqual(
            "",
            LLMExtractorOutputFormatInstructionsGenerator.render_semantic_consistency_constraints(
                schema
            ),
        )


class MetadataWrapperExceptionsTest(TestCase):
    """`render_metadata_wrapper_exceptions`."""

    def test_lists_list_exception_only_when_array_field_present(self) -> None:
        without_array = _build_schema(_field("note"))
        self.assertEqual(
            '- fields tagged "bare value", which you output directly',
            LLMExtractorOutputFormatInstructionsGenerator.render_metadata_wrapper_exceptions(
                without_array
            ),
        )

        with_array = _build_schema(
            _field(
                "employers",
                field_type="ARRAY_OF_STRUCT",
                primary_keys=["employer_name"],
                fields=[_field("employer_name")],
            ),
        )
        self.assertEqual(
            '- fields tagged "bare value", which you output directly\n'
            '- "list" fields, which are arrays whose elements are objects (each '
            "sub-field follows these same rules)",
            LLMExtractorOutputFormatInstructionsGenerator.render_metadata_wrapper_exceptions(
                with_array
            ),
        )


class ResultBranchShapeTest(TestCase):
    """The two pseudo-JSON INFERRED wrapper shapes."""

    def test_non_null_branch_uses_generic_value_placeholder(self) -> None:
        shape = (
            LLMExtractorOutputFormatInstructionsGenerator.render_nonnull_inferred_field_result_branch_shape()
        )
        self.assertIn('"value": <the extracted value>,', shape)
        self.assertIn(
            "at least one is required.",
            shape,
        )
        self.assertNotIn("null_reason", shape)

    def test_null_branch_lists_null_reason_union_and_omits_value(self) -> None:
        shape = (
            LLMExtractorOutputFormatInstructionsGenerator.render_null_inferred_field_result_branch_shape()
        )
        self.assertIn(
            '"null_reason": "not_applicable" | "no_info_found" | "explicitly_unknown",',
            shape,
        )
        self.assertIn('"citations": [', shape)
        self.assertNotIn('"value":', shape)


class DescribedEnumValuesTest(TestCase):
    """`render_described_enum_values` sources meanings from the enum itself."""

    def test_confidence_level_values(self) -> None:
        rendered = (
            LLMExtractorOutputFormatInstructionsGenerator.render_described_enum_values(
                ConfidenceLevel
            )
        )
        expected = "\n".join(
            f'- "{member.value}": {description}'
            for member, description in ConfidenceLevel.get_value_descriptions().items()
        )
        self.assertEqual(expected, rendered)

    def test_null_reason_values(self) -> None:
        rendered = (
            LLMExtractorOutputFormatInstructionsGenerator.render_described_enum_values(
                NullReason
            )
        )
        expected = "\n".join(
            f'- "{member.value}": {description}'
            for member, description in NullReason.get_value_descriptions().items()
        )
        self.assertEqual(expected, rendered)
