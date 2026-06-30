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
"""Tests for llm_request_output_schema.py.

Focuses on what the container adds over the field model: the reserved/
auto-injected `is_relevant` field, the user-defined vs all-fields split, and
name uniqueness/lookup. Field-level parsing/ordering is covered in
llm_request_output_schema_field_test.py.
"""
import re
from typing import Any
from unittest import TestCase

from recidiviz.documents.extraction.models.llm_request_output_schema import (
    IS_RELEVANT_FIELD_NAME,
    LLMRequestOutputSchema,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field import (
    ApplicableWhenValueConstraint,
    ConfidenceLevel,
    LLMOutputFieldMode,
    LLMOutputFieldType,
)
from recidiviz.utils.yaml_dict import YAMLDict

_DESCRIPTION = "A description that is long enough to be meaningful."
_COLLECTION_DESCRIPTION = "Extract employment information from case notes."


def _field(name: str, *, field_type: str = "STRING", **extra: Any) -> dict[str, Any]:
    return {"name": name, "type": field_type, "description": _DESCRIPTION, **extra}


def _enum_values(*names: str) -> list[dict[str, str]]:
    return [{"name": name, "description": f"{_DESCRIPTION} ({name})"} for name in names]


def _build_schema(
    *user_fields: dict[str, Any],
    collection_description: str = _COLLECTION_DESCRIPTION,
    **output_schema_extra: Any,
) -> LLMRequestOutputSchema:
    output_schema_dict: dict[str, Any] = {
        "full_batch_description": _DESCRIPTION,
        "result_level_description": _DESCRIPTION,
        "inferred_fields": list(user_fields),
        **output_schema_extra,
    }
    return LLMRequestOutputSchema.from_yaml_dict(
        yaml_dict=YAMLDict(output_schema_dict),
        collection_description=collection_description,
        default_minimum_confidence_level=ConfidenceLevel.INFERRED,
    )


class LLMRequestOutputSchemaTest(TestCase):
    """Tests for the output schema container."""

    def test_reserved_is_relevant_name_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Output schema may not define a [is_relevant] field — it is "
                "auto-injected by the framework."
            ),
        ):
            _build_schema(_field("is_relevant", field_type="BOOLEAN"))

    def test_is_relevant_field_shape(self) -> None:
        schema = _build_schema(
            _field("primary_status", field_type="ENUM", values=_enum_values("x"))
        )
        is_relevant = schema.is_relevant_field
        self.assertEqual(IS_RELEVANT_FIELD_NAME, is_relevant.name)
        self.assertEqual(LLMOutputFieldType.BOOLEAN, is_relevant.field_type)
        self.assertEqual(LLMOutputFieldMode.STRUCTURAL, is_relevant.field_mode)
        self.assertTrue(is_relevant.required)
        self.assertIn(_COLLECTION_DESCRIPTION, is_relevant.description)

    def test_all_fields_prepends_is_relevant_then_user_fields_in_order(self) -> None:
        schema = _build_schema(_field("a"), _field("b"), _field("c"))
        self.assertEqual(
            ["is_relevant", "a", "b", "c"],
            [field.name for field in schema.all_fields],
        )
        self.assertEqual(
            ["a", "b", "c"], [field.name for field in schema.user_defined_fields]
        )

    def test_duplicate_top_level_field_names_raise(self) -> None:
        # Raised by the scope builder before the container is constructed.
        with self.assertRaisesRegex(
            ValueError,
            re.escape("Output schema scope declares duplicate field name: [a]."),
        ):
            _build_schema(_field("a"), _field("a"))

    def test_get_field_returns_user_field(self) -> None:
        schema = _build_schema(_field("a"), _field("b"))
        self.assertEqual("a", schema.get_field("a").name)

    def test_get_field_unknown_name_raises(self) -> None:
        schema = _build_schema(_field("a"))
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Output schema has no top-level field named [ghost]. Declared "
                "fields: ['a']."
            ),
        ):
            schema.get_field("ghost")

    def test_get_field_does_not_resolve_is_relevant(self) -> None:
        # is_relevant is injected into all_fields but is NOT a user-defined field,
        # so it isn't addressable via get_field (entity-group resolution can't
        # accidentally bind to it).
        schema = _build_schema(_field("a"))
        with self.assertRaisesRegex(
            ValueError, re.escape("Output schema has no top-level field named")
        ):
            schema.get_field(IS_RELEVANT_FIELD_NAME)

    def test_top_level_fields_built_in_dependency_order(self) -> None:
        # Confirms the schema delegates to build_output_schema_fields rather than
        # building fields independently — `employer` references `status` declared
        # after it.
        schema = _build_schema(
            _field("employer", applicable_when_value={"status": ["employed"]}),
            _field(
                "status",
                field_type="ENUM",
                values=_enum_values("employed", "unemployed"),
            ),
        )
        constraint = schema.get_field("employer").semantic_consistency_constraints[0]
        assert isinstance(constraint, ApplicableWhenValueConstraint)
        self.assertIs(schema.get_field("status"), constraint.condition_field)

    def test_unexpected_key_in_output_schema_block_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape("Found unexpected config values in output_schema block:"),
        ):
            _build_schema(_field("a"), unexpected_key="value")
