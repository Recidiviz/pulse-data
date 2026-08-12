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
"""The semantic-consistency check: validates a result's field values against the
semantic-consistency constraints declared on the output schema, one
`ValidationIssue` per violated constraint.
"""

import abc
from typing import Any, Generic, TypeVar

import attr

from recidiviz.common import attr_validators
from recidiviz.documents.extraction.models.llm_request_output_schema_field import (
    ApplicableWhenNonnullConstraint,
    ApplicableWhenValueConstraint,
    ArrayOfStructLLMRequestOutputSchemaField,
    LLMOutputSemanticConsistencyConstraint,
    LLMRequestOutputSchemaField,
    NotApplicableWhenValueConstraint,
    RequiredWhenNonnullConstraint,
    RequiredWhenValueConstraint,
)
from recidiviz.documents.extraction.models.llm_request_output_values import (
    LLMRequestOutputValues,
)
from recidiviz.documents.extraction.validation.llm_document_validation_result import (
    ValidationCheckType,
    ValidationIssue,
)


def _value_is_present(value: Any) -> bool:
    """Returns whether |value| counts as present for semantic-consistency
    purposes: an array value (a list) is present when non-empty; any other value
    is present when non-null.

    This deliberately diverges from `LLMRequestOutputValues.is_value_present`
    (where an empty array is an affirmative value): constraints are rendered
    into the prompt in emptiness terms for a list-typed field — "leave it
    empty", "must be empty", "must have at least one entry" — so the check
    holds the output to exactly what the prompt asked for.
    """
    if isinstance(value, list):
        return bool(value)
    return value is not None


_ConstraintT = TypeVar("_ConstraintT", bound=LLMOutputSemanticConsistencyConstraint)


class _SemanticConstraintValidator(abc.ABC, Generic[_ConstraintT]):
    """Pass/fail logic and violation-message construction for one
    semantic-consistency constraint type, evaluated against the values at a
    single schema level.
    """

    @abc.abstractmethod
    def is_valid_output(
        self,
        *,
        constraint: _ConstraintT,
        field_value: Any,
        sibling_values: dict[str, Any],
    ) -> bool:
        """Returns whether the constrained field's |field_value| satisfies
        |constraint|, given |sibling_values| — the value of every field at the
        same schema level, keyed by field name.
        """

    @abc.abstractmethod
    def _violation_detail(
        self,
        *,
        constraint: _ConstraintT,
        field_display_name: str,
        sibling_values: dict[str, Any],
    ) -> str:
        """Returns the human-readable description of how |field_display_name|'s
        value violates |constraint|, reading the condition field's value from
        |sibling_values|.
        """

    def violation_detail(
        self,
        *,
        constraint: _ConstraintT,
        field_value: Any,
        field_display_name: str,
        sibling_values: dict[str, Any],
    ) -> str | None:
        """Returns the violation message when |field_value| fails |constraint|,
        or None when it satisfies it.
        """
        if self.is_valid_output(
            constraint=constraint,
            field_value=field_value,
            sibling_values=sibling_values,
        ):
            return None
        return self._violation_detail(
            constraint=constraint,
            field_display_name=field_display_name,
            sibling_values=sibling_values,
        )


class _ApplicableWhenNonnullValidator(
    _SemanticConstraintValidator[ApplicableWhenNonnullConstraint]
):
    """The field may only be present when its condition field is present."""

    def is_valid_output(
        self,
        *,
        constraint: ApplicableWhenNonnullConstraint,
        field_value: Any,
        sibling_values: dict[str, Any],
    ) -> bool:
        if not _value_is_present(field_value):
            return True
        return _value_is_present(sibling_values[constraint.condition_field.name])

    def _violation_detail(
        self,
        *,
        constraint: ApplicableWhenNonnullConstraint,
        field_display_name: str,
        sibling_values: dict[str, Any],
    ) -> str:
        condition_name = constraint.condition_field.name
        return (
            f"Field [{field_display_name}] is set but condition field "
            f"[{condition_name}] is null; only allowed when "
            f"[{condition_name}] is non-null."
        )


class _RequiredWhenNonnullValidator(
    _SemanticConstraintValidator[RequiredWhenNonnullConstraint]
):
    """The field must be present when its condition field is present."""

    def is_valid_output(
        self,
        *,
        constraint: RequiredWhenNonnullConstraint,
        field_value: Any,
        sibling_values: dict[str, Any],
    ) -> bool:
        if not _value_is_present(sibling_values[constraint.condition_field.name]):
            return True
        return _value_is_present(field_value)

    def _violation_detail(
        self,
        *,
        constraint: RequiredWhenNonnullConstraint,
        field_display_name: str,
        sibling_values: dict[str, Any],
    ) -> str:
        condition_name = constraint.condition_field.name
        return (
            f"Field [{field_display_name}] is not set but condition field "
            f"[{condition_name}] is non-null; required when "
            f"[{condition_name}] is non-null."
        )


class _ApplicableWhenValueValidator(
    _SemanticConstraintValidator[ApplicableWhenValueConstraint]
):
    """The field may only be present when its ENUM condition field is one of
    the constraint's values.
    """

    def is_valid_output(
        self,
        *,
        constraint: ApplicableWhenValueConstraint,
        field_value: Any,
        sibling_values: dict[str, Any],
    ) -> bool:
        if not _value_is_present(field_value):
            return True
        return sibling_values[constraint.condition_field.name] in constraint.values

    def _violation_detail(
        self,
        *,
        constraint: ApplicableWhenValueConstraint,
        field_display_name: str,
        sibling_values: dict[str, Any],
    ) -> str:
        condition_name = constraint.condition_field.name
        return (
            f"Field [{field_display_name}] is set but condition field "
            f"[{condition_name}] has value "
            f"[{sibling_values[condition_name]}]; only allowed when "
            f"[{condition_name}] is one of {constraint.values}."
        )


class _NotApplicableWhenValueValidator(
    _SemanticConstraintValidator[NotApplicableWhenValueConstraint]
):
    """The field must be absent when its ENUM condition field is one of the
    constraint's values.
    """

    def is_valid_output(
        self,
        *,
        constraint: NotApplicableWhenValueConstraint,
        field_value: Any,
        sibling_values: dict[str, Any],
    ) -> bool:
        if not _value_is_present(field_value):
            return True
        return sibling_values[constraint.condition_field.name] not in constraint.values

    def _violation_detail(
        self,
        *,
        constraint: NotApplicableWhenValueConstraint,
        field_display_name: str,
        sibling_values: dict[str, Any],
    ) -> str:
        condition_name = constraint.condition_field.name
        return (
            f"Field [{field_display_name}] is set but condition field "
            f"[{condition_name}] has value "
            f"[{sibling_values[condition_name]}]; not allowed when "
            f"[{condition_name}] is one of {constraint.values}."
        )


class _RequiredWhenValueValidator(
    _SemanticConstraintValidator[RequiredWhenValueConstraint]
):
    """The field must be present when its ENUM condition field is one of the
    constraint's values.
    """

    def is_valid_output(
        self,
        *,
        constraint: RequiredWhenValueConstraint,
        field_value: Any,
        sibling_values: dict[str, Any],
    ) -> bool:
        if sibling_values[constraint.condition_field.name] not in constraint.values:
            return True
        return _value_is_present(field_value)

    def _violation_detail(
        self,
        *,
        constraint: RequiredWhenValueConstraint,
        field_display_name: str,
        sibling_values: dict[str, Any],
    ) -> str:
        condition_name = constraint.condition_field.name
        return (
            f"Field [{field_display_name}] is not set but condition field "
            f"[{condition_name}] has value "
            f"[{sibling_values[condition_name]}]; required when "
            f"[{condition_name}] is one of {constraint.values}."
        )


_VALIDATORS_BY_CONSTRAINT_TYPE: dict[
    type[LLMOutputSemanticConsistencyConstraint],
    _SemanticConstraintValidator[Any],
] = {
    ApplicableWhenNonnullConstraint: _ApplicableWhenNonnullValidator(),
    RequiredWhenNonnullConstraint: _RequiredWhenNonnullValidator(),
    ApplicableWhenValueConstraint: _ApplicableWhenValueValidator(),
    NotApplicableWhenValueConstraint: _NotApplicableWhenValueValidator(),
    RequiredWhenValueConstraint: _RequiredWhenValueValidator(),
}


def _validator_for(
    constraint: LLMOutputSemanticConsistencyConstraint,
) -> _SemanticConstraintValidator[Any]:
    """Returns the validator implementing pass/fail logic and violation
    messages for |constraint|'s type.
    """
    if type(constraint) not in _VALIDATORS_BY_CONSTRAINT_TYPE:
        raise ValueError(
            f"Unexpected semantic-consistency constraint type: [{type(constraint)}]"
        )
    return _VALIDATORS_BY_CONSTRAINT_TYPE[type(constraint)]


@attr.define(frozen=True, kw_only=True)
class _SiblingScope:
    """The fields declared at one schema level of one result — the top level, or
    a single element of an ARRAY_OF_STRUCT field — together with the value each
    of those fields took there. A semantic-consistency constraint only ever
    relates siblings at the same schema level, so every constraint is evaluated
    within exactly one scope.
    """

    field_display_prefix: str = attr.ib(validator=attr_validators.is_str)
    """Prefix prepended to a field's name to form its display name in issues —
    empty at the top level, `"<array_field>[<index>]."` within an array element.
    """

    fields: list[LLMRequestOutputSchemaField] = attr.ib(
        validator=attr_validators.is_list_of(LLMRequestOutputSchemaField)
    )
    """The fields the schema declares at this level."""

    sibling_values: dict[str, Any] = attr.ib(validator=attr_validators.is_dict)
    """The value each field at this level took, keyed by field name. A scalar
    field maps to its unwrapped value; an array field to its list (of element
    dicts, for ARRAY_OF_STRUCT); an absent field to None.
    """


class SemanticConsistencyCheck:
    """The semantic-consistency check: validates a result's field values against
    the `applicable_when_nonnull` / `required_when_nonnull` /
    `applicable_when_value` / `not_applicable_when_value` /
    `required_when_value` constraints declared on the output schema, one
    `ValidationIssue` per violated constraint.

    A constraint only ever conditions on a sibling field at the same schema
    level, so evaluation walks a flat list of `_SiblingScope`s: one for the
    top-level fields, and one per element of each ARRAY_OF_STRUCT field. There
    is no third, recursive case: an ARRAY_OF_STRUCT field may not itself have an
    ARRAY_OF_STRUCT sub-field, so schema level never nests more than one deep.
    """

    @classmethod
    def issues(cls, *, output: LLMRequestOutputValues) -> list[ValidationIssue]:
        """Returns one `ValidationIssue` per semantic-consistency constraint
        |output| violates, or an empty list when it satisfies all of them.
        """
        issues: list[ValidationIssue] = []
        for scope in cls._sibling_scopes(output):
            for field in scope.fields:
                for constraint in field.semantic_consistency_constraints:
                    field_display_name = f"{scope.field_display_prefix}{field.name}"
                    detail = _validator_for(constraint).violation_detail(
                        constraint=constraint,
                        field_value=scope.sibling_values[field.name],
                        field_display_name=field_display_name,
                        sibling_values=scope.sibling_values,
                    )
                    if detail is None:
                        continue
                    issues.append(
                        ValidationIssue(
                            check_type=ValidationCheckType.SEMANTIC_CONSISTENCY,
                            field_name=field_display_name,
                            detail=detail,
                        )
                    )
        return issues

    @staticmethod
    def _sibling_scopes(output: LLMRequestOutputValues) -> list[_SiblingScope]:
        """Returns one scope per schema level |output| carries values for: the
        top level, plus every element of every ARRAY_OF_STRUCT field. A level
        where no field declares a constraint is skipped without reading its
        values at all.
        """
        top_level_fields = output.output_schema.all_fields
        scopes: list[_SiblingScope] = []
        if any(f.semantic_consistency_constraints for f in top_level_fields):
            scopes.append(
                _SiblingScope(
                    field_display_prefix="",
                    fields=top_level_fields,
                    sibling_values=output.values_dict,
                )
            )
        for field in top_level_fields:
            if not isinstance(field, ArrayOfStructLLMRequestOutputSchemaField):
                continue
            if not any(f.semantic_consistency_constraints for f in field.fields):
                continue
            for index, element in enumerate(output.array_elements(field=field) or []):
                scopes.append(
                    _SiblingScope(
                        field_display_prefix=f"{field.name}[{index}].",
                        fields=field.fields,
                        sibling_values=element,
                    )
                )
        return scopes
