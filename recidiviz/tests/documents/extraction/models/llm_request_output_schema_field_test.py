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
    DescribedEnum,
    EnumLLMRequestOutputSchemaField,
    LLMOutputFieldMode,
    LLMOutputFieldType,
    LLMRequestOutputSchemaField,
    NotApplicableWhenValueConstraint,
    NullReason,
    ScalarLLMRequestOutputSchemaField,
    description_with_enum_value_guidance,
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


class ConstraintResolutionTest(TestCase):
    """Constraint references hold resolved field objects; value conditions are
    enum-only and single-field.
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
            "  - employed: Has a job.\n"
            "  - unemployed: Has no job.",
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
            "Trailing whitespace here. Allowed values:\n  - a: The a value.",
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
            "  - not_applicable: The field does not apply given the values of "
            "other fields (e.g. `employer_name` when `primary_status` is "
            '"unemployed").\n'
            "  - no_info_found: The document does not mention this information.\n"
            "  - explicitly_unknown: The document acknowledges the information "
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
