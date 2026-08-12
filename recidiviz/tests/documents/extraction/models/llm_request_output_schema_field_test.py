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

import attr
from google.cloud import bigquery

from recidiviz.documents.extraction.models.llm_request_output_schema_field import (
    RESERVED_OUTPUT_SCHEMA_FIELD_NAMES,
    ApplicableWhenNonnullConstraint,
    ApplicableWhenValueConstraint,
    ArrayOfIntegerLLMRequestOutputSchemaField,
    ArrayOfStructLLMRequestOutputSchemaField,
    ConfidenceLevel,
    DescribedEnum,
    EnumLLMRequestOutputSchemaField,
    LLMOutputFieldMode,
    LLMOutputFieldType,
    LLMRequestOutputSchemaField,
    NotApplicableWhenValueConstraint,
    NullReason,
    PrimitiveScalarLLMRequestOutputSchemaField,
    RequiredWhenNonnullConstraint,
    RequiredWhenValueConstraint,
    ScalarValuedLLMRequestOutputSchemaField,
    description_with_enum_value_guidance,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field_names import (
    IS_RELEVANT_FIELD_NAME,
)
from recidiviz.utils.yaml_dict import YAMLDict

_DESCRIPTION = "A description that is long enough to be meaningful."


def _field(name: str, *, field_type: str = "STRING", **extra: Any) -> dict[str, Any]:
    """Returns a raw field-definition dict for use as one entry of a scope."""
    return {"name": name, "type": field_type, "description": _DESCRIPTION, **extra}


def _enum_values(*names: str) -> list[dict[str, str]]:
    """Returns raw `{name, description}` enum value dicts for an ENUM field. Each
    description is unique per name, since a field's value descriptions must be
    distinct.
    """
    return [{"name": name, "description": f"{_DESCRIPTION} ({name})"} for name in names]


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


class ReservedFieldNamesTest(TestCase):
    """Field names that collide with parsed-view columns are rejected by the
    field constructor itself. `is_relevant` is the one exemption at this level —
    the framework constructs the injected is_relevant field — and is instead
    rejected wherever user fields are aggregated (see the array sub-field test
    here and the schema-container tests in llm_request_output_schema_test.py).
    """

    def test_every_reserved_name_raises_at_parse(self) -> None:
        for reserved_name in sorted(
            RESERVED_OUTPUT_SCHEMA_FIELD_NAMES - {IS_RELEVANT_FIELD_NAME}
        ):
            with self.subTest(reserved_name=reserved_name):
                with self.assertRaisesRegex(
                    ValueError,
                    re.escape(
                        f"Output schema declares field [{reserved_name}], which "
                        f"collides with a column the parsed extraction-result "
                        f"views emit."
                    ),
                ):
                    _build(_field(reserved_name))

    def test_reserved_name_raises_on_direct_construction(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Output schema declares field [person_id], which collides with a "
                "column the parsed extraction-result views emit."
            ),
        ):
            PrimitiveScalarLLMRequestOutputSchemaField(
                name="person_id",
                description=_DESCRIPTION,
                required=False,
                inferred_field_config=None,
                scalar_type=LLMOutputFieldType.STRING,
            )

    def test_reserved_name_raises_as_array_sub_field(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Output schema declares field [source_array_index], which collides "
                "with a column the parsed extraction-result views emit."
            ),
        ):
            _build(
                _field(
                    "jobs",
                    field_type="ARRAY_OF_STRUCT",
                    primary_keys=["employer"],
                    fields=[_field("employer"), _field("source_array_index")],
                )
            )

    def test_is_relevant_raises_as_array_sub_field(self) -> None:
        # is_relevant passes the field-level validator (the framework constructs
        # the injected is_relevant field), so the array's own sub-field check must
        # reject it.
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "ARRAY_OF_STRUCT field [jobs] declares a sub-field named "
                "[is_relevant], which collides with a column the parsed "
                "extraction-result views emit."
            ),
        ):
            _build(
                _field(
                    "jobs",
                    field_type="ARRAY_OF_STRUCT",
                    primary_keys=["employer"],
                    fields=[
                        _field("employer"),
                        _field("is_relevant", field_type="BOOLEAN"),
                    ],
                )
            )


class ConstraintResolutionTest(TestCase):
    """Constraint references hold resolved field objects; value conditions are
    enum-only and single-field, and nonnull conditions are scalar-only.
    """

    def test_value_condition_builds_correct_subclass_holding_enum_field(self) -> None:
        fields = _build(
            _field(
                "status",
                field_type="ENUM",
                values=_enum_values("employed", "unemployed"),
            ),
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
                _field(
                    "status",
                    field_type="ENUM",
                    values=_enum_values("employed", "unemployed"),
                ),
                _field("employer", applicable_when_value={"status": ["retired"]}),
            )

    def test_nonnull_condition_builds_holding_scalar_field(self) -> None:
        rate_amount, rate_period = _build(
            _field("rate_amount", field_type="FLOAT"),
            _field(
                "rate_period",
                applicable_when_nonnull="rate_amount",
                required_when_nonnull="rate_amount",
            ),
        )
        applicable, required = rate_period.semantic_consistency_constraints
        assert isinstance(applicable, ApplicableWhenNonnullConstraint)
        assert isinstance(required, RequiredWhenNonnullConstraint)

        self.assertIs(rate_amount, applicable.condition_field)
        self.assertIs(rate_amount, required.condition_field)

    def test_nonnull_condition_on_array_field_raises(self) -> None:
        # An array has no null branch to be non-null against, and the prompt
        # renders a nonnull condition as a bare "is set" with no emptiness
        # wording — so the model would never learn that `[]` means not-set.
        # Conditioning on an array needs its own emptiness-based constraint.
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Expected field [assignments] to be scalar-valued, but it has "
                "type [ARRAY_OF_STRUCT]."
            ),
        ):
            _build(
                _field(
                    "assignments",
                    field_type="ARRAY_OF_STRUCT",
                    primary_keys=["assignment_name"],
                    fields=[_field("assignment_name")],
                ),
                _field("assignment_note", applicable_when_nonnull="assignments"),
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
                _field("status", field_type="ENUM", values=_enum_values("employed")),
                _field("tenure", field_type="ENUM", values=_enum_values("long")),
                _field(
                    "employer",
                    applicable_when_value={"status": ["employed"], "tenure": ["long"]},
                ),
            )


class RealisticConstraintUsageTest(TestCase):
    """Worked examples of every semantic-consistency constraint on one realistic
    employment schema, plus the two composition idioms we build from them. Reads
    as the reference for how the `*_when_*` field keys are meant to be used.

    The constraints (single field):
      - `applicable_when_value`     — the field may only be non-null for certain
                                      values of an ENUM sibling.
      - `not_applicable_when_value` — the inverse: the field must be null for
                                      certain values of an ENUM sibling.
      - `applicable_when_nonnull`   — the field may only be non-null when a sibling
                                      is itself non-null.

    The idioms (pair a gating constraint with a requiring one on the same sibling):
      - "set iff a value"  = `applicable_when_value` + `required_when_value`
      - "both or neither"  = `applicable_when_nonnull` + `required_when_nonnull`
    """

    @staticmethod
    def _employment_fields() -> dict[str, LLMRequestOutputSchemaField]:
        """Returns a realistic employment schema exercising every constraint,
        keyed by field name.
        """
        fields = _build(
            _field(
                "primary_status",
                field_type="ENUM",
                values=_enum_values("employed", "unemployed", "other"),
            ),
            # Only relevant when the person is employed.
            _field(
                "employer_name",
                applicable_when_value={"primary_status": ["employed"]},
            ),
            # Benefits information does not apply when the person is employed.
            _field(
                "benefits_type",
                not_applicable_when_value={"primary_status": ["employed"]},
            ),
            # "Set iff unemployed": gated to `unemployed` AND required for it.
            _field(
                "unemployment_reason",
                applicable_when_value={"primary_status": ["unemployed"]},
                required_when_value={"primary_status": ["unemployed"]},
            ),
            _field("pay_rate_amount", field_type="FLOAT"),
            # "Both or neither": a period only when there is an amount, and
            # required whenever there is one.
            _field(
                "pay_rate_period",
                field_type="ENUM",
                values=_enum_values("hourly", "weekly", "monthly"),
                applicable_when_nonnull="pay_rate_amount",
                required_when_nonnull="pay_rate_amount",
            ),
        )
        return {field.name: field for field in fields}

    def test_applicable_when_value(self) -> None:
        fields = self._employment_fields()
        (constraint,) = fields["employer_name"].semantic_consistency_constraints
        assert isinstance(constraint, ApplicableWhenValueConstraint)
        self.assertIs(fields["primary_status"], constraint.condition_field)
        self.assertEqual(["employed"], constraint.values)

    def test_not_applicable_when_value(self) -> None:
        fields = self._employment_fields()
        (constraint,) = fields["benefits_type"].semantic_consistency_constraints
        assert isinstance(constraint, NotApplicableWhenValueConstraint)
        self.assertIs(fields["primary_status"], constraint.condition_field)
        self.assertEqual(["employed"], constraint.values)

    def test_set_iff_a_value_pairs_applicable_and_required(self) -> None:
        constraints = self._employment_fields()[
            "unemployment_reason"
        ].semantic_consistency_constraints
        applicable = next(
            c for c in constraints if isinstance(c, ApplicableWhenValueConstraint)
        )
        required = next(
            c for c in constraints if isinstance(c, RequiredWhenValueConstraint)
        )
        self.assertEqual(["unemployed"], applicable.values)
        self.assertEqual(["unemployed"], required.values)
        self.assertEqual("primary_status", applicable.condition_field.name)
        self.assertEqual("primary_status", required.condition_field.name)

    def test_both_or_neither_pairs_applicable_and_required_nonnull(self) -> None:
        fields = self._employment_fields()
        constraints = fields["pay_rate_period"].semantic_consistency_constraints
        applicable = next(
            c for c in constraints if isinstance(c, ApplicableWhenNonnullConstraint)
        )
        required = next(
            c for c in constraints if isinstance(c, RequiredWhenNonnullConstraint)
        )
        self.assertIs(fields["pay_rate_amount"], applicable.condition_field)
        self.assertIs(fields["pay_rate_amount"], required.condition_field)


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
                _field("status", field_type="ENUM", values=_enum_values("employed")),
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
                _field("status", field_type="ENUM", values=_enum_values("employed")),
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
                    values=_enum_values("employed", "employed", "unemployed"),
                )
            )

    def test_enum_with_duplicate_value_descriptions_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "ENUM field [status] declares duplicate value descriptions: "
                "['The very same description.']."
            ),
        ):
            _build(
                _field(
                    "status",
                    field_type="ENUM",
                    values=[
                        {
                            "name": "employed",
                            "description": "The very same description.",
                        },
                        {
                            "name": "unemployed",
                            "description": "The very same description.",
                        },
                    ],
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
                    _field(
                        "employment_type",
                        field_type="ENUM",
                        values=_enum_values("ft", "pt"),
                    ),
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
                _field("status", field_type="ENUM", values=_enum_values("employed")),
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
    """LLMOutputFieldType value-type helpers and ConfidenceLevel ordering."""

    def test_is_primitive_scalar_value_type_for_every_member(self) -> None:
        # Never raises for any member, and classifies as expected.
        expected_scalar = {
            LLMOutputFieldType.STRING,
            LLMOutputFieldType.BOOLEAN,
            LLMOutputFieldType.INTEGER,
            LLMOutputFieldType.FLOAT,
        }
        for field_type in LLMOutputFieldType:
            self.assertEqual(
                field_type in expected_scalar,
                field_type.is_primitive_scalar_value_type(),
            )

    def test_primitive_scalar_value_type(self) -> None:
        # A primitive is its own value type; an ENUM's value is a STRING.
        self.assertEqual(
            LLMOutputFieldType.INTEGER,
            LLMOutputFieldType.INTEGER.primitive_scalar_value_type(),
        )
        self.assertEqual(
            LLMOutputFieldType.STRING,
            LLMOutputFieldType.ENUM.primitive_scalar_value_type(),
        )
        with self.assertRaisesRegex(ValueError, r"has no scalar value type"):
            LLMOutputFieldType.ARRAY_OF_STRUCT.primitive_scalar_value_type()

    def test_primitive_scalar_value_big_query_type(self) -> None:
        self.assertEqual(
            bigquery.SqlTypeNames.STRING,
            LLMOutputFieldType.ENUM.primitive_scalar_value_big_query_type(),
        )
        self.assertEqual(
            bigquery.SqlTypeNames.FLOAT,
            LLMOutputFieldType.FLOAT.primitive_scalar_value_big_query_type(),
        )
        with self.assertRaisesRegex(ValueError, r"has no scalar value type"):
            LLMOutputFieldType.ARRAY_OF_STRUCT.primitive_scalar_value_big_query_type()

    def test_scalar_field_type_dispatch(self) -> None:
        for field_type in LLMOutputFieldType:
            if not field_type.is_primitive_scalar_value_type():
                continue
            (field,) = _build(_field("f", field_type=field_type.value))
            self.assertIsInstance(field, PrimitiveScalarLLMRequestOutputSchemaField)
            self.assertEqual(field_type, field.field_type)

    def test_enum_and_array_dispatch(self) -> None:
        enum_field, array_field = _build(
            _field("e", field_type="ENUM", values=_enum_values("x")),
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


class ScalarValuedFieldTest(TestCase):
    """The ScalarValued hierarchy and json_path_for_value."""

    def test_scalar_and_enum_are_scalar_valued_array_is_not(self) -> None:
        scalar, enum_field, array_field = _build(
            _field("s"),
            _field("e", field_type="ENUM", values=_enum_values("x")),
            _field(
                "a",
                field_type="ARRAY_OF_STRUCT",
                primary_keys=["s"],
                fields=[_field("s")],
            ),
        )
        self.assertIsInstance(scalar, ScalarValuedLLMRequestOutputSchemaField)
        self.assertIsInstance(enum_field, ScalarValuedLLMRequestOutputSchemaField)
        self.assertNotIsInstance(array_field, ScalarValuedLLMRequestOutputSchemaField)
        # The json_path_for_value helper lives on the scalar-valued base only.
        self.assertFalse(hasattr(array_field, "json_path_for_value"))

    def test_json_path_for_value(self) -> None:
        # An inferred field wraps its value under `.value`; a structural field
        # holds the bare value at its key. Covers both a primitive scalar and an
        # enum, since the helper lives on their shared base.
        inferred_scalar, structural_scalar, inferred_enum = _build(
            _field("employer"),
            _field("note", field_mode="STRUCTURAL"),
            _field("status", field_type="ENUM", values=_enum_values("open", "closed")),
        )
        for field, expected in [
            (inferred_scalar, "$.employer.value"),
            (structural_scalar, "$.note"),
            (inferred_enum, "$.status.value"),
        ]:
            with self.subTest(field=field.name):
                assert isinstance(field, ScalarValuedLLMRequestOutputSchemaField)
                self.assertEqual(expected, field.json_path_for_value())


class IsInferredFieldTest(TestCase):
    """is_inferred_field on scalar fields and the ARRAY_OF_STRUCT override."""

    def test_scalar_inferred_vs_structural(self) -> None:
        inferred_scalar, structural_scalar = _build(
            _field("employer"),
            _field("note", field_mode="STRUCTURAL"),
        )
        self.assertTrue(inferred_scalar.is_inferred_field)
        self.assertFalse(structural_scalar.is_inferred_field)

    def test_array_is_inferred_iff_any_sub_field_is_inferred(self) -> None:
        (array_with_inferred_sub,) = _build(
            _field(
                "jobs",
                field_type="ARRAY_OF_STRUCT",
                primary_keys=["employer"],
                fields=[
                    _field("employer", field_mode="STRUCTURAL"),
                    _field("pay_rate", field_type="FLOAT"),
                ],
            ),
        )
        self.assertTrue(array_with_inferred_sub.is_inferred_field)

        (array_all_structural,) = _build(
            _field(
                "jobs",
                field_type="ARRAY_OF_STRUCT",
                field_mode="STRUCTURAL",
                primary_keys=["employer"],
                fields=[_field("employer", field_mode="STRUCTURAL")],
            ),
        )
        self.assertFalse(array_all_structural.is_inferred_field)


class EnumFieldDescriptionTest(TestCase):
    """`EnumLLMRequestOutputSchemaField.description` bakes in each value's meaning."""

    def test_description_bakes_in_value_guidance(self) -> None:
        (status,) = _build(
            _field(
                "status",
                field_type="ENUM",
                values=[
                    {"name": "employed", "description": "Has a job."},
                    {"name": "unemployed", "description": "Has no job."},
                ],
            )
        )
        self.assertEqual(
            f"{_DESCRIPTION.rstrip('.')}. Allowed values:\n"
            "- employed: Has a job.\n"
            "- unemployed: Has no job.",
            status.description,
        )

    def test_description_normalizes_trailing_whitespace_and_period(self) -> None:
        # Folded YAML scalars (`>`) arrive with a trailing newline — it must not
        # leak into the baked description as "...\n. Allowed values".
        (status,) = _build(
            _field(
                "status",
                field_type="ENUM",
                description="Trailing whitespace here.\n",
                values=[{"name": "a", "description": "The a value."}],
            )
        )
        self.assertEqual(
            "Trailing whitespace here. Allowed values:\n- a: The a value.",
            status.description,
        )

    def test_value_descriptions_are_stripped(self) -> None:
        # Folded YAML scalars (`>`) arrive with surrounding whitespace on each
        # value description too — it must not leak into the rendered bullet.
        (status,) = _build(
            _field(
                "status",
                field_type="ENUM",
                values=[
                    {"name": "employed", "description": "  Has a job.\n"},
                    {"name": "unemployed", "description": "Has no job.\n"},
                ],
            )
        )
        self.assertEqual(
            f"{_DESCRIPTION.rstrip('.')}. Allowed values:\n"
            "- employed: Has a job.\n"
            "- unemployed: Has no job.",
            status.description,
        )


class _PartiallyDescribedEnum(DescribedEnum):
    """A DescribedEnum whose `get_value_descriptions` omits a member, to exercise
    the missing-description guard.
    """

    A = "a"
    B = "b"

    @classmethod
    def get_value_descriptions(cls) -> dict[DescribedEnum, str]:
        return {cls.A: "The A value."}


class DescribedEnumGuidanceTest(TestCase):
    """`description_with_enum_value_guidance` and the built-in DescribedEnums."""

    def test_bakes_member_descriptions_in_definition_order(self) -> None:
        self.assertEqual(
            "Why no value could be extracted. Allowed values:\n"
            "- not_applicable: The field does not apply given the values of the "
            "other fields it depends on — for example, a field that is only "
            "meaningful for a particular value of another field, when that value "
            "does not hold.\n"
            "- no_info_found: The document does not mention this information.\n"
            "- explicitly_unknown: The document acknowledges the information "
            "but states it is unknown.",
            description_with_enum_value_guidance(
                description="Why no value could be extracted.", enum_cls=NullReason
            ),
        )

    def test_missing_value_description_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Enum [_PartiallyDescribedEnum] is missing a value description for "
                "member [B]."
            ),
        ):
            description_with_enum_value_guidance(
                description="Some description.", enum_cls=_PartiallyDescribedEnum
            )

    def test_builtin_described_enums_cover_every_member(self) -> None:
        # Each member must have a description, or schema generation would raise.
        for enum_cls in (ConfidenceLevel, NullReason):
            with self.subTest(enum_cls=enum_cls.__name__):
                description_with_enum_value_guidance(
                    description="A description.", enum_cls=enum_cls
                )


class ArrayFieldTypeTest(TestCase):
    """The array field types synthesized by the framework: ARRAY_OF_INTEGER and
    the optional min_items on ARRAY_OF_STRUCT. Both are built directly (not from
    YAML) — they are synthesized by the entity-resolution schema builder.
    """

    def test_array_of_integer_field_type_and_min_items(self) -> None:
        field = ArrayOfIntegerLLMRequestOutputSchemaField(
            name="entry_nums",
            description=_DESCRIPTION,
            required=True,
            inferred_field_config=None,
            min_items=1,
        )
        self.assertEqual(LLMOutputFieldType.ARRAY_OF_INTEGER, field.field_type)
        self.assertFalse(field.field_type.is_primitive_scalar_value_type())
        self.assertEqual(LLMOutputFieldMode.STRUCTURAL, field.field_mode)
        self.assertEqual(1, field.min_items)

    def test_array_of_integer_min_items_defaults_to_none(self) -> None:
        field = ArrayOfIntegerLLMRequestOutputSchemaField(
            name="entry_nums",
            description=_DESCRIPTION,
            required=True,
            inferred_field_config=None,
        )
        self.assertIsNone(field.min_items)

    def test_array_of_integer_non_positive_min_items_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Field [min_items] on [ArrayOfIntegerLLMRequestOutputSchemaField] "
                "must be a positive integer. Found value [0]"
            ),
        ):
            ArrayOfIntegerLLMRequestOutputSchemaField(
                name="entry_nums",
                description=_DESCRIPTION,
                required=True,
                inferred_field_config=None,
                min_items=0,
            )

    def test_array_of_integer_has_no_primitive_scalar_value_type(self) -> None:
        with self.assertRaisesRegex(ValueError, re.escape("has no scalar value type")):
            LLMOutputFieldType.ARRAY_OF_INTEGER.primitive_scalar_value_type()

    def test_array_of_struct_min_items_defaults_to_none(self) -> None:
        field = ArrayOfStructLLMRequestOutputSchemaField(
            name="entities",
            description=_DESCRIPTION,
            required=True,
            inferred_field_config=None,
            fields=[
                PrimitiveScalarLLMRequestOutputSchemaField(
                    name="a",
                    description=_DESCRIPTION,
                    required=True,
                    inferred_field_config=None,
                    scalar_type=LLMOutputFieldType.STRING,
                )
            ],
            primary_keys=["a"],
        )
        self.assertIsNone(field.min_items)

    def test_array_of_struct_non_positive_min_items_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Field [min_items] on [ArrayOfStructLLMRequestOutputSchemaField] "
                "must be a positive integer. Found value [-1]"
            ),
        ):
            ArrayOfStructLLMRequestOutputSchemaField(
                name="entities",
                description=_DESCRIPTION,
                required=True,
                inferred_field_config=None,
                fields=[
                    PrimitiveScalarLLMRequestOutputSchemaField(
                        name="a",
                        description=_DESCRIPTION,
                        required=True,
                        inferred_field_config=None,
                        scalar_type=LLMOutputFieldType.STRING,
                    )
                ],
                primary_keys=["a"],
                min_items=-1,
            )


class NullableFieldTest(TestCase):
    """The `nullable` flag: default, and the STRUCTURAL-only invariant."""

    def test_nullable_defaults_to_false(self) -> None:
        (field,) = _build(_field("f"))
        self.assertFalse(field.nullable)

    def test_nullable_allowed_on_structural_field(self) -> None:
        (field,) = _build(_field("f", field_mode="STRUCTURAL"))
        self.assertTrue(attr.evolve(field, nullable=True).nullable)

    def test_nullable_on_inferred_field_raises(self) -> None:
        # An INFERRED field encodes absence via its null_reason branch, so it may
        # not also be nullable.
        (field,) = _build(_field("f"))
        with self.assertRaisesRegex(
            ValueError,
            re.escape("Field [f] is nullable but INFERRED"),
        ):
            attr.evolve(field, nullable=True)
