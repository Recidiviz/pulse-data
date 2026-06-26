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
"""One field of an LLM extractor collection's output schema, plus the
semantic-consistency constraints between fields.

A field varies along two orthogonal dimensions:

- its **type** (`LLMOutputFieldType`) — scalar, enum, or array-of-struct —
  determines which type-specific attributes it carries. This dimension is
  modeled by subclassing `LLMRequestOutputSchemaField`: `ScalarLLMRequestOutputSchemaField`,
  `EnumLLMRequestOutputSchemaField`, and `ArrayOfStructLLMRequestOutputSchemaField` each declare
  exactly the attributes their type needs.
- its **mode** (`LLMOutputFieldMode`) — INFERRED (extracted from text, wrapped in
  companion metadata) or STRUCTURAL (a bare value the model assigns) — determines
  whether it carries a minimum confidence level and semantic-consistency
  constraints. This dimension is modeled by composition: an INFERRED field holds
  an `InferredFieldConfig`, a STRUCTURAL field holds `None`.

A semantic-consistency constraint holds a direct reference to the sibling field
it conditions on (not just its name). Because a field's constraints reference
other fields, a scope of sibling fields must be built in dependency order —
`build_output_schema_fields` does this, rejecting cycles. Holding the
referenced field directly lets each constraint validate itself (e.g. a value
condition stores the `EnumLLMRequestOutputSchemaField` it conditions on and checks its
values against that field's allowed values), so there is no separate
cross-field resolution pass.
"""
import abc
from enum import Enum
from typing import Self

import attr

from recidiviz.big_query.big_query_attr_validators import (
    is_valid_unquoted_bq_identifier,
)
from recidiviz.common import attr_validators, recidiviz_attr_validators
from recidiviz.utils.yaml_dict import YAMLDict


class LLMOutputFieldType(Enum):
    """The value type of an output schema field, as declared by the `type` key
    in a field definition.
    """

    STRING = "STRING"
    BOOLEAN = "BOOLEAN"
    ENUM = "ENUM"
    INTEGER = "INTEGER"
    FLOAT = "FLOAT"
    ARRAY_OF_STRUCT = "ARRAY_OF_STRUCT"

    def is_scalar_type(self) -> bool:
        """Returns whether this is a single-value scalar type — i.e. not ENUM
        (which carries allowed values) or ARRAY_OF_STRUCT (which carries
        sub-fields).
        """
        if self in (
            LLMOutputFieldType.STRING,
            LLMOutputFieldType.BOOLEAN,
            LLMOutputFieldType.INTEGER,
            LLMOutputFieldType.FLOAT,
        ):
            return True
        if self in (LLMOutputFieldType.ENUM, LLMOutputFieldType.ARRAY_OF_STRUCT):
            return False

        raise ValueError(f"Unexpected LLMOutputFieldType {self}")


class LLMOutputFieldMode(Enum):
    """How an output schema field is produced by the model."""

    INFERRED = "INFERRED"
    """The value is extracted from the document text and wrapped in companion
    metadata (confidence level, citations, adversarial interpretation,
    null reason).
    """

    STRUCTURAL = "STRUCTURAL"
    """The value is a bare value assigned by the model rather than extracted
    from text (e.g. `entity_id` in entity resolution) — no companion metadata.
    """


class ConfidenceLevel(Enum):
    """Discrete label indicating the evidence quality behind an extraction."""

    # NOTE: These are listed in order from least confident to most confident. DO NOT
    #  CHANGE THE ORDER OF THESE ENUMS RELATIVE TO EACH OTHER.
    SPECULATIVE = "speculative"
    """Lowest confidence: a plausible alternative reading of the document exists
    (i.e. the model recorded an adversarial interpretation), so there is genuine
    doubt about whether the extracted value is correct.
    """

    INFERRED = "inferred"
    """The value follows from one clear inferential step over direct evidence in
    the document; there is no adversarial interpretation.
    """

    EXPLICIT = "explicit"
    """The value is directly stated in the document, with at most minor
    normalization (synonym, abbreviation, or paraphrase).
    """

    VERBATIM = "verbatim"
    """Highest confidence: the value appears exactly as-is in the document — the
    citation text IS the value, with zero rewording or substitution.
    """

    @property
    def rank(self) -> int:
        """Returns this level's position in the ordinal ordering (0 = lowest
        confidence).
        """
        return list(ConfidenceLevel).index(self)

    def meets_minimum(self, minimum: "ConfidenceLevel") -> bool:
        """Returns whether this level is at least as confident as |minimum|."""
        return self.rank >= minimum.rank


class NullReason(Enum):
    """Why an INFERRED field has no extracted value. Returned by the model on the
    null branch of a field's output, and the allowed values of the generated
    JSON Schema's `null_reason` property.
    """

    NOT_APPLICABLE = "not_applicable"
    """The field does not apply given the values of other fields (e.g.
    `employer_name` when `primary_status` is "unemployed").
    """

    NO_INFO_FOUND = "no_info_found"
    """The document does not mention this information."""

    EXPLICITLY_UNKNOWN = "explicitly_unknown"
    """The document acknowledges the information but states it is unknown."""


@attr.define(frozen=True, kw_only=True)
class LLMOutputSemanticConsistencyConstraint(abc.ABC):
    """A constraint on when an output schema field may be non-null, conditioned
    on the value of a sibling field at the same schema level. Constraints are
    one-directional: they restrict when a field can be non-null but never
    require it to be non-null, so a null field always satisfies them. Rendered
    into natural language in the prompt AND enforced programmatically in
    validation. Each subclass holds a direct reference to its condition field.
    """


@attr.define(frozen=True, kw_only=True)
class InferredFieldConfig:
    """The configuration an INFERRED output schema field carries beyond its
    type. Its presence on a field IS the INFERRED signal; a STRUCTURAL field
    holds `None` instead.
    """

    minimum_confidence_level: ConfidenceLevel = attr.ib(
        validator=attr.validators.in_(ConfidenceLevel)
    )
    """Minimum ordinal confidence level for this field's value to survive into
    validated output. Resolved at parse time from the per-field override or the
    collection default.
    """

    semantic_consistency_constraints: list[
        LLMOutputSemanticConsistencyConstraint
    ] = attr.ib(
        validator=attr_validators.is_list_of(LLMOutputSemanticConsistencyConstraint)
    )
    """Semantic-consistency constraints restricting when this field may be
    non-null. Empty when the field is unconditionally applicable.
    """


@attr.define(frozen=True, kw_only=True)
class LLMRequestOutputSchemaField(abc.ABC):
    """One field in an extractor collection's output schema. Subclasses fix the
    field's type; the optional `inferred_field_config` fixes its mode.
    """

    name: str = attr.ib(
        validator=[is_valid_unquoted_bq_identifier, attr_validators.is_snake_case]
    )
    """Field identifier. Must be a valid unquoted BigQuery identifier (it becomes
    a column and/or struct sub-field name in downstream views) and snake_case.
    """

    description: str = attr.ib(
        validator=recidiviz_attr_validators.is_meaningful_description
    )
    """What this field represents. Included in the prompt."""

    required: bool = attr.ib(validator=attr_validators.is_bool)
    """Whether this field must be non-null when the document is relevant. A
    required field may not also declare a semantic-consistency constraint — it
    cannot be both unconditionally required and conditionally applicable.
    """

    inferred_field_config: InferredFieldConfig | None = attr.ib(
        validator=attr_validators.is_opt(InferredFieldConfig)
    )
    """Set iff this field is INFERRED (extracted from text with companion
    metadata); `None` for a STRUCTURAL field.
    """

    @property
    @abc.abstractmethod
    def field_type(self) -> LLMOutputFieldType:
        """Returns the value type of this field."""

    @property
    def field_mode(self) -> LLMOutputFieldMode:
        """Returns whether this field is INFERRED or STRUCTURAL."""
        return (
            LLMOutputFieldMode.INFERRED
            if self.inferred_field_config is not None
            else LLMOutputFieldMode.STRUCTURAL
        )

    @property
    def semantic_consistency_constraints(
        self,
    ) -> list[LLMOutputSemanticConsistencyConstraint]:
        """Returns this field's semantic-consistency constraints (empty for a
        STRUCTURAL field).
        """
        return (
            self.inferred_field_config.semantic_consistency_constraints
            if self.inferred_field_config
            else []
        )

    @property
    def minimum_confidence_level(self) -> ConfidenceLevel | None:
        """Returns this field's minimum confidence level, or `None` for a
        STRUCTURAL field.
        """
        return (
            self.inferred_field_config.minimum_confidence_level
            if self.inferred_field_config
            else None
        )

    def __attrs_post_init__(self) -> None:
        if self.required and self.semantic_consistency_constraints:
            raise ValueError(
                f"Field [{self.name}] is required but also declares "
                f"semantic-consistency constraint(s) — a field cannot be both "
                f"unconditionally required and conditionally applicable."
            )

    @staticmethod
    def _constraint_dependency_names(
        field_name: str, field_yaml_dict: YAMLDict
    ) -> set[str]:
        """Returns the names of the fields the constraints in |field_yaml_dict|
        reference, read without consuming them (so the field can still be fully parsed
        later). Used to order a scope's fields by dependency.
        """
        dependency_names: set[str] = set()
        if (
            nonnull_condition_field := field_yaml_dict.peek_optional(
                "applicable_when_nonnull", str
            )
        ) is not None:
            dependency_names.add(nonnull_condition_field)
        for value_condition_key in (
            "applicable_when_value",
            "not_applicable_when_value",
        ):
            if (
                value_condition := field_yaml_dict.peek_optional(
                    value_condition_key, dict
                )
            ) is not None:
                dependency_names.update(value_condition.keys())
        if field_name in dependency_names:
            raise ValueError(
                f"Field [{field_name}] declares a semantic-consistency constraint "
                f"conditioned on itself."
            )
        return dependency_names

    @classmethod
    def build_output_schema_fields(
        cls,
        *,
        field_yamls: list[YAMLDict],
        default_minimum_confidence_level: ConfidenceLevel,
    ) -> list["LLMRequestOutputSchemaField"]:
        """Builds a collection of sibling fields from their YAMLs, constructing
        each field only after the fields its semantic-consistency constraints
        reference, and returning them in their original (YAML) order. Raises on
        duplicate names, references to unknown fields in constraints, self-references,
        or dependency cycles.
        """
        names_in_order = []
        field_yaml_by_name = {}
        field_dependency_names_by_name = {}
        for field_yaml in field_yamls:
            field_name = field_yaml.peek("name", str)
            if field_name in field_yaml_by_name:
                raise ValueError(
                    f"Output schema scope declares duplicate field name: "
                    f"[{field_name}]."
                )
            names_in_order.append(field_name)
            field_yaml_by_name[field_name] = field_yaml
            field_dependency_names_by_name[
                field_name
            ] = cls._constraint_dependency_names(field_name, field_yaml)

        for field_name, dependency_names in field_dependency_names_by_name.items():
            if unknown_names := dependency_names - set(names_in_order):
                raise ValueError(
                    f"Field [{field_name}] declares a semantic-consistency constraint "
                    f"conditioned on unknown sibling field(s): "
                    f"{sorted(unknown_names)}."
                )

        built_fields_by_name: dict[str, LLMRequestOutputSchemaField] = {}
        unbuilt_names = set(names_in_order)
        while unbuilt_names:
            unblocked_fields_to_build = [
                field_name
                for field_name in unbuilt_names
                if field_dependency_names_by_name[field_name].issubset(
                    built_fields_by_name.keys()
                )
            ]
            if not unblocked_fields_to_build:
                raise ValueError(
                    f"Semantic-consistency constraints form a dependency cycle "
                    f"among output schema fields. Cannot build: "
                    f"{sorted(unbuilt_names)}."
                )
            for field_name in unblocked_fields_to_build:
                built_fields_by_name[field_name] = cls._build_from_yaml_dict(
                    yaml_dict=field_yaml_by_name[field_name],
                    default_minimum_confidence_level=default_minimum_confidence_level,
                    already_built_fields_by_name=built_fields_by_name,
                )
                unbuilt_names.remove(field_name)

        return [built_fields_by_name[field_name] for field_name in names_in_order]

    @classmethod
    def _build_from_yaml_dict(
        cls,
        *,
        yaml_dict: YAMLDict,
        default_minimum_confidence_level: ConfidenceLevel,
        already_built_fields_by_name: dict[str, "LLMRequestOutputSchemaField"],
    ) -> "LLMRequestOutputSchemaField":
        """Builds one field from its YAML, dispatching to the subclass that
        matches the declared `type` and resolving each semantic-consistency
        constraint against the already-built |already_built_fields_by_name|.
        """
        name = yaml_dict.pop("name", str)
        field_type = LLMOutputFieldType(yaml_dict.pop("type", str))
        field_mode = (
            LLMOutputFieldMode(mode_str)
            if (mode_str := yaml_dict.pop_optional("field_mode", str)) is not None
            else LLMOutputFieldMode.INFERRED
        )

        # The minimum_confidence_level and constraint keys are only consumed for
        # an INFERRED field. A STRUCTURAL field that declares them therefore
        # leaves them unconsumed, and the empty-dict check at the end of this
        # method rejects them — no separate STRUCTURAL guard is needed.
        inferred_field_config: InferredFieldConfig | None = None
        if field_mode is LLMOutputFieldMode.INFERRED:
            declared_minimum_confidence_level = yaml_dict.pop_optional(
                "minimum_confidence_level", str
            )
            inferred_field_config = InferredFieldConfig(
                minimum_confidence_level=(
                    ConfidenceLevel(declared_minimum_confidence_level)
                    if declared_minimum_confidence_level is not None
                    else default_minimum_confidence_level
                ),
                semantic_consistency_constraints=cls._pop_semantic_consistency_constraints(
                    yaml_dict, already_built_fields_by_name
                ),
            )

        description = yaml_dict.pop("description", str)
        required = yaml_dict.pop_optional("required", bool) or False

        field: LLMRequestOutputSchemaField
        if field_type.is_scalar_type():
            field = ScalarLLMRequestOutputSchemaField(
                name=name,
                description=description,
                required=required,
                inferred_field_config=inferred_field_config,
                scalar_type=field_type,
            )
        elif field_type is LLMOutputFieldType.ENUM:
            field = EnumLLMRequestOutputSchemaField(
                name=name,
                description=description,
                required=required,
                inferred_field_config=inferred_field_config,
                values=yaml_dict.pop_list("values", str),
            )
        elif field_type is LLMOutputFieldType.ARRAY_OF_STRUCT:
            field = ArrayOfStructLLMRequestOutputSchemaField(
                name=name,
                description=description,
                required=required,
                inferred_field_config=inferred_field_config,
                fields=cls.build_output_schema_fields(
                    field_yamls=yaml_dict.pop_dicts("fields"),
                    default_minimum_confidence_level=default_minimum_confidence_level,
                ),
                primary_keys=yaml_dict.pop_list("primary_keys", str),
            )
        else:
            raise ValueError(f"Unexpected field_type: [{field_type}]")

        if yaml_dict:
            raise ValueError(
                f"Found unexpected config values for output schema field "
                f"[{name}]: {repr(yaml_dict.get())}"
            )
        return field

    @classmethod
    def _pop_semantic_consistency_constraints(
        cls,
        yaml_dict: YAMLDict,
        already_built_fields_by_name: dict[str, "LLMRequestOutputSchemaField"],
    ) -> list[LLMOutputSemanticConsistencyConstraint]:
        """Returns the semantic-consistency constraints parsed off a field's
        YAML block, consuming the `applicable_when_nonnull`,
        `applicable_when_value`, and `not_applicable_when_value` keys and
        resolving each condition against the already-built
        |already_built_fields_by_name|.
        """
        constraints: list[LLMOutputSemanticConsistencyConstraint] = []
        if (
            nonnull_condition_field := yaml_dict.pop_optional(
                "applicable_when_nonnull", str
            )
        ) is not None:
            constraints.append(
                ApplicableWhenNonnullConstraint(
                    condition_field=already_built_fields_by_name[
                        nonnull_condition_field
                    ]
                )
            )
        if (
            applicable_when_value_yaml := yaml_dict.pop_dict_optional(
                "applicable_when_value"
            )
        ) is not None:
            constraints.append(
                ApplicableWhenValueConstraint.from_yaml_dict(
                    applicable_when_value_yaml, already_built_fields_by_name
                )
            )
        if (
            not_applicable_when_value_yaml := yaml_dict.pop_dict_optional(
                "not_applicable_when_value"
            )
        ) is not None:
            constraints.append(
                NotApplicableWhenValueConstraint.from_yaml_dict(
                    not_applicable_when_value_yaml, already_built_fields_by_name
                )
            )
        return constraints


def _is_scalar_field_type(
    _instance: object, _attribute: "attr.Attribute", value: LLMOutputFieldType
) -> None:
    if not isinstance(value, LLMOutputFieldType) or not value.is_scalar_type():
        raise ValueError(
            f"ScalarLLMRequestOutputSchemaField requires a scalar field type, received "
            f"[{value}]."
        )


@attr.define(frozen=True, kw_only=True)
class ScalarLLMRequestOutputSchemaField(LLMRequestOutputSchemaField):
    """An output schema field whose value is a single scalar (STRING, BOOLEAN,
    INTEGER, or FLOAT).
    """

    scalar_type: LLMOutputFieldType = attr.ib(validator=_is_scalar_field_type)
    """The scalar value type of this field."""

    @property
    def field_type(self) -> LLMOutputFieldType:
        return self.scalar_type


@attr.define(frozen=True, kw_only=True)
class EnumLLMRequestOutputSchemaField(LLMRequestOutputSchemaField):
    """An output schema field whose value is one of a fixed set of allowed enum
    values.
    """

    values: list[str] = attr.ib(
        validator=[attr_validators.is_non_empty_list, attr_validators.is_list_of(str)]
    )
    """Allowed enum values."""

    @values.validator
    def _check_values(self, _attribute: "attr.Attribute", value: list[str]) -> None:
        if duplicate_values := {v for v in value if value.count(v) > 1}:
            raise ValueError(
                f"ENUM field [{self.name}] declares duplicate allowed values: "
                f"{sorted(duplicate_values)}."
            )

    @property
    def field_type(self) -> LLMOutputFieldType:
        return LLMOutputFieldType.ENUM


@attr.define(frozen=True, kw_only=True)
class ArrayOfStructLLMRequestOutputSchemaField(LLMRequestOutputSchemaField):
    """An output schema field whose value is an array of structs, each with the
    same sub-fields (one level of nesting only).
    """

    fields: list[LLMRequestOutputSchemaField] = attr.ib(
        validator=[
            attr_validators.is_non_empty_list,
            attr_validators.is_list_of(LLMRequestOutputSchemaField),
        ]
    )
    """Sub-field definitions. A sub-field may not itself be an
    ARRAY_OF_STRUCT.
    """

    @fields.validator
    def _check_fields(
        self, _attribute: "attr.Attribute", value: list[LLMRequestOutputSchemaField]
    ) -> None:
        """Validates the field set for this particular ARRAY_OF_STRUCT field: names are
        unique and none is itself an ARRAY_OF_STRUCT (only one level of nesting is
        supported).
        """
        names = [field.name for field in value]
        if duplicate_names := {n for n in names if names.count(n) > 1}:
            raise ValueError(
                f"ARRAY_OF_STRUCT field [{self.name}] declares duplicate "
                f"sub-field names: {sorted(duplicate_names)}."
            )
        for field in value:
            if isinstance(field, ArrayOfStructLLMRequestOutputSchemaField):
                raise ValueError(
                    f"ARRAY_OF_STRUCT field [{self.name}] has sub-field "
                    f"[{field.name}] which is itself an ARRAY_OF_STRUCT — "
                    f"only one level of nesting is supported."
                )

    primary_keys: list[str] = attr.ib(
        validator=[attr_validators.is_non_empty_list, attr_validators.is_list_of(str)]
    )
    """Sub-field names used to pair expected vs. actual array elements during
    golden eval scoring.
    """

    @property
    def field_type(self) -> LLMOutputFieldType:
        return LLMOutputFieldType.ARRAY_OF_STRUCT

    @property
    def fields_by_name(self) -> dict[str, LLMRequestOutputSchemaField]:
        """Returns this array's sub-fields keyed by name. Names are unique (the
        `fields` validator rejects duplicates).
        """
        return {field.name: field for field in self.fields}

    def get_field(self, field_name: str) -> LLMRequestOutputSchemaField:
        """Returns the sub-field named |field_name|, raising if this array
        declares no such sub-field.
        """
        if field_name not in self.fields_by_name:
            raise ValueError(
                f"ARRAY_OF_STRUCT field [{self.name}] has no sub-field named "
                f"[{field_name}]. Declared sub-fields: "
                f"{sorted(self.fields_by_name)}."
            )
        return self.fields_by_name[field_name]

    def __attrs_post_init__(self) -> None:
        super().__attrs_post_init__()

        if unknown_keys := set(self.primary_keys) - set(self.fields_by_name):
            raise ValueError(
                f"ARRAY_OF_STRUCT field [{self.name}] declares primary_keys "
                f"{sorted(unknown_keys)} that are not among its sub-fields: "
                f"{sorted(self.fields_by_name)}."
            )


# Defined after the field subclasses so the value-condition constraints can
# reference EnumLLMRequestOutputSchemaField directly in their validators.
@attr.define(frozen=True, kw_only=True)
class ApplicableWhenNonnullConstraint(LLMOutputSemanticConsistencyConstraint):
    """The field may only be non-null when its condition field is also
    non-null.
    """

    condition_field: LLMRequestOutputSchemaField = attr.ib()
    """The sibling field that must be non-null for this field to be non-null."""

    @condition_field.validator
    def _check_condition_field(
        self, _attribute: "attr.Attribute", value: object
    ) -> None:
        # `attr.validators.instance_of` can't be used here because
        # LLMRequestOutputSchemaField is abstract (mypy `type-abstract`).
        if not isinstance(value, LLMRequestOutputSchemaField):
            raise ValueError(
                f"condition_field must be an output schema field, received "
                f"[{type(value)}]."
            )


@attr.define(frozen=True, kw_only=True)
class _EnumValueConditionConstraint(LLMOutputSemanticConsistencyConstraint):
    """Shared base for constraints conditioned on an ENUM sibling field taking
    (or not taking) one of a set of values.
    """

    condition_field: EnumLLMRequestOutputSchemaField = attr.ib(
        validator=attr.validators.instance_of(EnumLLMRequestOutputSchemaField)
    )
    """The ENUM sibling field whose value this constraint conditions on."""

    values: list[str] = attr.ib(
        validator=[attr_validators.is_non_empty_list, attr_validators.is_list_of(str)]
    )
    """The condition field values this constraint conditions on. Each must be
    one of the condition field's allowed values.
    """

    def __attrs_post_init__(self) -> None:
        if invalid_values := set(self.values) - set(self.condition_field.values):
            raise ValueError(
                f"Value condition on ENUM field [{self.condition_field.name}] "
                f"lists values {sorted(invalid_values)} that are not among its "
                f"allowed values: {self.condition_field.values}."
            )

    @classmethod
    def from_yaml_dict(
        cls,
        yaml_dict: YAMLDict,
        already_built_fields_by_name: dict[str, "LLMRequestOutputSchemaField"],
    ) -> Self:
        """Returns the constraint parsed from a single-field value-condition
        block (`{field_name: [values]}`), resolving the referenced field
        against |already_built_fields_by_name| and requiring it to be an ENUM field.
        Builds whichever concrete subclass it is called on.
        """
        condition_field_names = yaml_dict.keys()
        if len(condition_field_names) != 1:
            raise ValueError(
                f"A value condition must reference exactly one field, found: "
                f"{sorted(condition_field_names)}."
            )
        condition_field_name = condition_field_names[0]
        values = yaml_dict.pop_list(condition_field_name, str)
        condition_field = already_built_fields_by_name[condition_field_name]
        if not isinstance(condition_field, EnumLLMRequestOutputSchemaField):
            raise ValueError(
                f"A value condition must reference an ENUM field, but "
                f"[{condition_field_name}] has type "
                f"[{condition_field.field_type.value}]."
            )
        return cls(condition_field=condition_field, values=values)


@attr.define(frozen=True, kw_only=True)
class ApplicableWhenValueConstraint(_EnumValueConditionConstraint):
    """The field may only be non-null when its ENUM condition field is one of
    the listed values.
    """


@attr.define(frozen=True, kw_only=True)
class NotApplicableWhenValueConstraint(_EnumValueConditionConstraint):
    """The field must be null when its ENUM condition field is one of the
    listed values.
    """
