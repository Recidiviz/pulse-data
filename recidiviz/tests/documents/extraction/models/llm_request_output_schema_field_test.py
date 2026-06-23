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
"""Tests for llm_request_output_schema_field.py.

Focuses on the genuinely tricky behavior: dependency-ordered field construction,
constraint reference resolution + the enum-only tightening, and the mode/type
invariants no single-field validator can express. Builds field scopes from
inline `YAMLDict`s via `build_output_schema_fields`.
"""
import re
from typing import Any
from unittest import TestCase

from recidiviz.documents.extraction.models.llm_request_output_schema_field import (
    ApplicableWhenNonnullConstraint,
    ApplicableWhenValueConstraint,
    ArrayOfStructLLMRequestOutputSchemaField,
    ConfidenceLevel,
    EnumLLMRequestOutputSchemaField,
    LLMOutputFieldMode,
    LLMOutputFieldType,
    LLMRequestOutputSchemaField,
    NotApplicableWhenValueConstraint,
    ScalarLLMRequestOutputSchemaField,
)
from recidiviz.utils.yaml_dict import YAMLDict

_DESCRIPTION = "A description that is long enough to be meaningful."


def _field(name: str, *, field_type: str = "STRING", **extra: Any) -> dict[str, Any]:
    """Returns a raw field-definition dict for use as one entry of a scope."""
    return {"name": name, "type": field_type, "description": _DESCRIPTION, **extra}


def _build(
    *field_dicts: dict[str, Any],
    default_minimum_confidence_level: ConfidenceLevel = ConfidenceLevel.INFERRED,
) -> list[LLMRequestOutputSchemaField]:
    """Builds one scope of fields from raw field dicts."""
    return LLMRequestOutputSchemaField.build_output_schema_fields(
        field_yamls=[YAMLDict(field_dict) for field_dict in field_dicts],
        default_minimum_confidence_level=default_minimum_confidence_level,
    )


class BuildOrderingTest(TestCase):
    """Dependency-ordered construction via build_output_schema_fields."""

    def test_out_of_order_dependency_resolves_preserving_declaration_order(
        self,
    ) -> None:
        # `a` references `b`, declared after it — still builds, and the result
        # preserves declaration (not build) order.
        fields = _build(
            _field("a", applicable_when_nonnull="b"),
            _field("b"),
        )
        self.assertEqual(["a", "b"], [field.name for field in fields])
        constraint = fields[0].semantic_consistency_constraints[0]
        assert isinstance(constraint, ApplicableWhenNonnullConstraint)
        # The constraint holds the resolved field OBJECT, not a name.
        self.assertIs(fields[1], constraint.condition_field)

    def test_dependency_cycle_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Semantic-consistency constraints form a dependency cycle among "
                "output schema fields. Cannot build: ['a', 'b']."
            ),
        ):
            _build(
                _field("a", applicable_when_nonnull="b"),
                _field("b", applicable_when_nonnull="a"),
            )

    def test_self_reference_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Field [a] declares a semantic-consistency constraint conditioned "
                "on itself."
            ),
        ):
            _build(_field("a", applicable_when_nonnull="a"))

    def test_unknown_sibling_reference_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Field [a] declares a semantic-consistency constraint conditioned "
                "on unknown sibling field(s): ['ghost']."
            ),
        ):
            _build(_field("a", applicable_when_nonnull="ghost"))

    def test_duplicate_field_names_in_scope_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape("Output schema scope declares duplicate field name: [a]."),
        ):
            _build(_field("a"), _field("a"))


class ConstraintResolutionTest(TestCase):
    """Constraint references hold resolved field objects; value conditions are
    enum-only and single-field.
    """

    def test_value_condition_builds_correct_subclass_holding_enum_field(self) -> None:
        fields = _build(
            _field("status", field_type="ENUM", values=["employed", "unemployed"]),
            _field("employer", applicable_when_value={"status": ["employed"]}),
            _field("reason", not_applicable_when_value={"status": ["employed"]}),
        )
        status, employer, reason = fields
        applicable = employer.semantic_consistency_constraints[0]
        not_applicable = reason.semantic_consistency_constraints[0]
        assert isinstance(applicable, ApplicableWhenValueConstraint)
        assert isinstance(not_applicable, NotApplicableWhenValueConstraint)

        self.assertIs(status, applicable.condition_field)
        self.assertEqual(["employed"], applicable.values)
        self.assertIs(status, not_applicable.condition_field)

    def test_value_condition_on_non_enum_field_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "A value condition must reference an ENUM field, but [status] has "
                "type [STRING]."
            ),
        ):
            _build(
                _field("status"),
                _field("employer", applicable_when_value={"status": ["employed"]}),
            )

    def test_value_condition_with_value_not_in_enum_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Value condition on ENUM field [status] lists values ['retired'] "
                "that are not among its allowed values: ['employed', 'unemployed']."
            ),
        ):
            _build(
                _field("status", field_type="ENUM", values=["employed", "unemployed"]),
                _field("employer", applicable_when_value={"status": ["retired"]}),
            )

    def test_value_condition_referencing_multiple_fields_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "A value condition must reference exactly one field, found: "
                "['status', 'tenure']."
            ),
        ):
            _build(
                _field("status", field_type="ENUM", values=["employed"]),
                _field("tenure", field_type="ENUM", values=["long"]),
                _field(
                    "employer",
                    applicable_when_value={"status": ["employed"], "tenure": ["long"]},
                ),
            )


class FieldModeTest(TestCase):
    """INFERRED (holds InferredFieldConfig) vs STRUCTURAL (holds None)."""

    def test_required_field_with_constraint_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Field [employer] is required but also declares "
                "semantic-consistency constraint(s) — a field cannot be both "
                "unconditionally required and conditionally applicable."
            ),
        ):
            _build(
                _field("status", field_type="ENUM", values=["employed"]),
                _field(
                    "employer",
                    required=True,
                    applicable_when_value={"status": ["employed"]},
                ),
            )

    def test_structural_field_declaring_minimum_confidence_level_raises(self) -> None:
        # The key is only consumed in the INFERRED branch, so it survives to the
        # empty-dict check rather than needing a bespoke STRUCTURAL guard.
        with self.assertRaisesRegex(
            ValueError,
            re.escape("Found unexpected config values for output schema field [note]"),
        ):
            _build(
                _field(
                    "note", field_mode="STRUCTURAL", minimum_confidence_level="explicit"
                )
            )

    def test_structural_field_declaring_constraint_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Found unexpected config values for output schema field [employer]"
            ),
        ):
            _build(
                _field("status", field_type="ENUM", values=["employed"]),
                _field(
                    "employer",
                    field_mode="STRUCTURAL",
                    applicable_when_value={"status": ["employed"]},
                ),
            )

    def test_inferred_minimum_confidence_resolution(self) -> None:
        # Per-field override wins; otherwise the passed-in default is used.
        overridden, defaulted = _build(
            _field("pay_rate", field_type="FLOAT", minimum_confidence_level="explicit"),
            _field("job_title"),
            default_minimum_confidence_level=ConfidenceLevel.SPECULATIVE,
        )
        self.assertEqual(ConfidenceLevel.EXPLICIT, overridden.minimum_confidence_level)
        self.assertEqual(
            ConfidenceLevel.SPECULATIVE, defaulted.minimum_confidence_level
        )
        self.assertEqual(LLMOutputFieldMode.INFERRED, defaulted.field_mode)

    def test_structural_field_has_no_inferred_config(self) -> None:
        (note,) = _build(_field("note", field_mode="STRUCTURAL"))
        self.assertEqual(LLMOutputFieldMode.STRUCTURAL, note.field_mode)
        self.assertIsNone(note.inferred_field_config)
        self.assertIsNone(note.minimum_confidence_level)
        self.assertEqual([], note.semantic_consistency_constraints)


class TypeInvariantsTest(TestCase):
    """Type-specific subclass invariants (the interesting ones)."""

    def test_enum_with_duplicate_values_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "ENUM field [status] declares duplicate allowed values: "
                "['employed']."
            ),
        ):
            _build(
                _field(
                    "status",
                    field_type="ENUM",
                    values=["employed", "employed", "unemployed"],
                )
            )

    def test_array_of_struct_with_nested_array_sub_field_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "ARRAY_OF_STRUCT field [employers] has sub-field [history] which "
                "is itself an ARRAY_OF_STRUCT — only one level of nesting is "
                "supported."
            ),
        ):
            _build(
                _field(
                    "employers",
                    field_type="ARRAY_OF_STRUCT",
                    primary_keys=["name"],
                    fields=[
                        _field("name"),
                        _field(
                            "history",
                            field_type="ARRAY_OF_STRUCT",
                            primary_keys=["role"],
                            fields=[_field("role")],
                        ),
                    ],
                )
            )

    def test_array_of_struct_primary_keys_not_among_sub_fields_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "ARRAY_OF_STRUCT field [employers] declares primary_keys ['ghost'] "
                "that are not among its sub-fields: ['name']."
            ),
        ):
            _build(
                _field(
                    "employers",
                    field_type="ARRAY_OF_STRUCT",
                    primary_keys=["ghost"],
                    fields=[_field("name")],
                )
            )

    def test_array_sub_fields_form_their_own_dependency_scope(self) -> None:
        # A constraint between two sub-fields resolves at the inner scope.
        (employers,) = _build(
            _field(
                "employers",
                field_type="ARRAY_OF_STRUCT",
                primary_keys=["name"],
                fields=[
                    _field("employment_type", field_type="ENUM", values=["ft", "pt"]),
                    _field("name", applicable_when_value={"employment_type": ["ft"]}),
                ],
            )
        )
        assert isinstance(employers, ArrayOfStructLLMRequestOutputSchemaField)
        name_sub_field = employers.get_field("name")
        constraint = name_sub_field.semantic_consistency_constraints[0]
        assert isinstance(constraint, ApplicableWhenValueConstraint)
        self.assertIs(
            employers.get_field("employment_type"), constraint.condition_field
        )

    def test_array_sub_field_constraint_referencing_top_level_field_raises(
        self,
    ) -> None:
        # The top-level `status` is not a sibling at the array's inner scope.
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Field [name] declares a semantic-consistency constraint "
                "conditioned on unknown sibling field(s): ['status']."
            ),
        ):
            _build(
                _field("status", field_type="ENUM", values=["employed"]),
                _field(
                    "employers",
                    field_type="ARRAY_OF_STRUCT",
                    primary_keys=["name"],
                    fields=[
                        _field("name", applicable_when_value={"status": ["employed"]})
                    ],
                ),
            )

    def test_array_get_field_unknown_sub_field_raises(self) -> None:
        (employers,) = _build(
            _field(
                "employers",
                field_type="ARRAY_OF_STRUCT",
                primary_keys=["name"],
                fields=[_field("name")],
            )
        )
        assert isinstance(employers, ArrayOfStructLLMRequestOutputSchemaField)
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "ARRAY_OF_STRUCT field [employers] has no sub-field named [ghost]. "
                "Declared sub-fields: ['name']."
            ),
        ):
            employers.get_field("ghost")


class EnumHelpersTest(TestCase):
    """LLMOutputFieldType.is_scalar_type and ConfidenceLevel ordering."""

    def test_is_scalar_type_for_every_member(self) -> None:
        # Never raises for any member, and classifies as expected.
        expected_scalar = {
            LLMOutputFieldType.STRING,
            LLMOutputFieldType.BOOLEAN,
            LLMOutputFieldType.INTEGER,
            LLMOutputFieldType.FLOAT,
        }
        for field_type in LLMOutputFieldType:
            self.assertEqual(field_type in expected_scalar, field_type.is_scalar_type())

    def test_scalar_field_type_dispatch(self) -> None:
        for field_type in LLMOutputFieldType:
            if not field_type.is_scalar_type():
                continue
            (field,) = _build(_field("f", field_type=field_type.value))
            self.assertIsInstance(field, ScalarLLMRequestOutputSchemaField)
            self.assertEqual(field_type, field.field_type)

    def test_enum_and_array_dispatch(self) -> None:
        enum_field, array_field = _build(
            _field("e", field_type="ENUM", values=["x"]),
            _field(
                "a",
                field_type="ARRAY_OF_STRUCT",
                primary_keys=["s"],
                fields=[_field("s")],
            ),
        )
        self.assertIsInstance(enum_field, EnumLLMRequestOutputSchemaField)
        self.assertIsInstance(array_field, ArrayOfStructLLMRequestOutputSchemaField)

    def test_confidence_level_rank_reflects_declaration_order(self) -> None:
        self.assertEqual(
            [
                ConfidenceLevel.SPECULATIVE,
                ConfidenceLevel.INFERRED,
                ConfidenceLevel.EXPLICIT,
                ConfidenceLevel.VERBATIM,
            ],
            sorted(ConfidenceLevel, key=lambda level: level.rank),
        )

    def test_confidence_level_meets_minimum(self) -> None:
        self.assertTrue(
            ConfidenceLevel.EXPLICIT.meets_minimum(ConfidenceLevel.INFERRED)
        )
        self.assertTrue(
            ConfidenceLevel.INFERRED.meets_minimum(ConfidenceLevel.INFERRED)
        )
        self.assertFalse(
            ConfidenceLevel.SPECULATIVE.meets_minimum(ConfidenceLevel.INFERRED)
        )
