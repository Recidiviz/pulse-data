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

import attr

from recidiviz.documents.extraction.models.llm_request_output_schema import (
    IS_RELEVANT_FIELD_NAME,
    LLMRequestOutputSchema,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field import (
    ApplicableWhenValueConstraint,
    ConfidenceLevel,
    LLMOutputFieldMode,
    LLMOutputFieldType,
    PrimitiveScalarLLMRequestOutputSchemaField,
)
from recidiviz.utils.types import assert_type
from recidiviz.utils.yaml_dict import YAMLDict

_DESCRIPTION = "A description that is long enough to be meaningful."
_RELEVANCE_CRITERIA = "Whether the document mentions employment information"


def _field(name: str, *, field_type: str = "STRING", **extra: Any) -> dict[str, Any]:
    return {"name": name, "type": field_type, "description": _DESCRIPTION, **extra}


def _enum_values(*names: str) -> list[dict[str, str]]:
    return [{"name": name, "description": f"{_DESCRIPTION} ({name})"} for name in names]


def _build_schema(
    *user_fields: dict[str, Any],
    relevance_criteria: str = _RELEVANCE_CRITERIA,
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
        relevance_criteria=relevance_criteria,
        default_minimum_confidence_level=ConfidenceLevel.INFERRED,
    )


class LLMRequestOutputSchemaTest(TestCase):
    """Tests for the output schema container."""

    def test_reserved_is_relevant_name_raises(self) -> None:
        # is_relevant passes the field-level reserved-name validator (the
        # framework constructs the injected is_relevant field), so the container
        # must reject a user-defined one.
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Output schema may not define a [is_relevant] field — it is "
                "auto-injected by the framework."
            ),
        ):
            _build_schema(_field("is_relevant", field_type="BOOLEAN"))

    def test_directly_constructed_is_relevant_field_raises(self) -> None:
        # Constructing the schema without going through the YAML parse still
        # rejects a user-defined is_relevant field, via __attrs_post_init__.
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Output schema may not define a [is_relevant] field — it is "
                "auto-injected by the framework."
            ),
        ):
            valid_schema = _build_schema(_field("a"))
            LLMRequestOutputSchema(
                full_batch_description=_DESCRIPTION,
                result_level_description=_DESCRIPTION,
                relevance_criteria=_RELEVANCE_CRITERIA,
                user_defined_fields=[
                    assert_type(
                        valid_schema.is_relevant_field,
                        PrimitiveScalarLLMRequestOutputSchemaField,
                    )
                ],
            )

    def test_is_relevant_field_shape(self) -> None:
        schema = _build_schema(
            _field("primary_status", field_type="ENUM", values=_enum_values("x"))
        )
        is_relevant = schema.is_relevant_field
        assert is_relevant is not None
        self.assertEqual(IS_RELEVANT_FIELD_NAME, is_relevant.name)
        self.assertEqual(LLMOutputFieldType.BOOLEAN, is_relevant.field_type)
        self.assertEqual(LLMOutputFieldMode.STRUCTURAL, is_relevant.field_mode)
        self.assertTrue(is_relevant.required)
        self.assertEqual(_RELEVANCE_CRITERIA, is_relevant.description)

    def test_all_fields_prepends_is_relevant_then_user_fields_in_order(self) -> None:
        schema = _build_schema(_field("a"), _field("b"), _field("c"))
        self.assertEqual(
            ["is_relevant", "a", "b", "c"],
            [field.name for field in schema.all_fields],
        )
        self.assertEqual(
            ["a", "b", "c"], [field.name for field in schema.user_defined_fields]
        )

    def test_has_inferred_fields(self) -> None:
        # True when any user-defined field is INFERRED (the default), False when
        # every user-defined field is STRUCTURAL (the entity-resolution shape). The
        # framework-injected is_relevant field is STRUCTURAL and does not count.
        self.assertTrue(_build_schema(_field("a")).has_inferred_fields)
        self.assertFalse(
            _build_schema(
                _field("a", field_mode="STRUCTURAL"),
                _field("b", field_mode="STRUCTURAL"),
            ).has_inferred_fields
        )

    def test_none_relevance_criteria_has_no_is_relevant_field(self) -> None:
        # A None relevance_criteria declares a schema with no is_relevant field
        # (the entity-resolution shape); the property returns None and all_fields
        # collapses to just the user-defined fields.
        schema = attr.evolve(
            _build_schema(_field("a"), _field("b")), relevance_criteria=None
        )
        self.assertIsNone(schema.is_relevant_field)
        self.assertEqual(["a", "b"], [field.name for field in schema.all_fields])
        self.assertEqual(
            [field.name for field in schema.user_defined_fields],
            [field.name for field in schema.all_fields],
        )

    def test_reserved_is_relevant_name_raises_even_when_relevance_free(self) -> None:
        # The reserved-name check is independent of relevance_criteria — a
        # user-defined `is_relevant` is rejected even for a relevance-free schema.
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Output schema may not define a [is_relevant] field — it is "
                "auto-injected by the framework."
            ),
        ):
            LLMRequestOutputSchema(
                full_batch_description=_DESCRIPTION,
                result_level_description=_DESCRIPTION,
                relevance_criteria=None,
                user_defined_fields=[
                    PrimitiveScalarLLMRequestOutputSchemaField(
                        name=IS_RELEVANT_FIELD_NAME,
                        description=_DESCRIPTION,
                        required=False,
                        inferred_field_config=None,
                        scalar_type=LLMOutputFieldType.BOOLEAN,
                    )
                ],
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


class FieldTypeAccessorsTest(TestCase):
    """Tests for the scalar_valued / array_of_struct user-field accessors."""

    def test_partitions_fields_by_type_preserving_order(self) -> None:
        schema = _build_schema(
            _field("a"),
            _field(
                "jobs",
                field_type="ARRAY_OF_STRUCT",
                primary_keys=["employer"],
                fields=[_field("employer")],
            ),
            _field("b", field_type="ENUM", values=_enum_values("x", "y")),
        )
        self.assertEqual(["a", "b"], [f.name for f in schema.scalar_valued_user_fields])
        self.assertEqual(["jobs"], [f.name for f in schema.array_of_struct_user_fields])

    def test_no_array_fields(self) -> None:
        schema = _build_schema(_field("a"))
        self.assertEqual(["a"], [f.name for f in schema.scalar_valued_user_fields])
        self.assertEqual([], schema.array_of_struct_user_fields)
