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
"""Conditions that decide when a law regime rule applies, parsed from the when
mappings in law regime YAML files. For example, with Nevada's sentence fields,

    when:
      qualified_for_enhanced_good_time: true
      any_of:
        - offense_category: [A, B]
        - offense_group: { not_in: [PROPERTY, DRUG] }

parses into

    AllOf(conditions=[
        FieldCondition(
            field="qualified_for_enhanced_good_time",
            operator=ConditionOperator.EQUALS,
            value=True,
        ),
        AnyOf(conditions=[
            FieldCondition(
                field="offense_category",
                operator=ConditionOperator.IN,
                value=[NevadaOffenseCategory.CATEGORY_A, NevadaOffenseCategory.CATEGORY_B],
            ),
            FieldCondition(
                field="offense_group",
                operator=ConditionOperator.NOT_IN,
                value=[NevadaOffenseGroup.PROPERTY, NevadaOffenseGroup.DRUG],
            ),
        ]),
    ])

A field set to a single value means equals, a field set to a list means in, and a
field set to a one-key mapping names any ConditionOperator. Several keys in one
mapping must all hold, all_of holds when every one of its conditions does,
any_of holds when one of its conditions does, and condition: <name> refers to a
named condition defined elsewhere. A mapping can't repeat a key, so two
conditions on the same field or two named conditions that must both hold go in
an all_of list.

Values are parsed into the field's type, so an unknown field or a value the field
can't take fails when the file loads.

A sentence with no value for a field never meets equals or in, and always meets
not_equals and not_in, so offense_id: { not_in: [1576] } matches a sentence with
an unknown offense ID. This supports fields where None is an expected value, e.g.
parent_sentence_id. The operators that order values, every operator besides
equals, not_equals, in, and not_in, raise on a missing value. Conditions don't
check that a sentence has every field a rule needs; that check happens where
sentences are validated.

PyYAML's SafeLoader keeps only the last value of a repeated key, so the YAML
must be loaded with a loader that rejects repeated keys, or a condition can be
silently dropped. The law regime loader does this.

"""

import datetime
import types
import typing
from collections.abc import Callable, Iterable, Mapping
from enum import Enum
from typing import Any, Union

import attr

from recidiviz.common import attr_validators
from recidiviz.sentence_calculator.models.inputs import Sentence
from recidiviz.utils.types import assert_type
from recidiviz.utils.yaml_dict import YAMLDict

# The key in a when mapping that holds conditions that must all hold.
ALL_OF_KEY = "all_of"
# The key in a when mapping that holds conditions of which at least one must hold.
ANY_OF_KEY = "any_of"
# The key in a when mapping that refers to a named condition.
NAMED_CONDITION_KEY = "condition"

# The types a field must have for conditions to compare against it.
_COMPARABLE_FIELD_TYPES: tuple[type, ...] = (bool, int, float, str, datetime.date)

# A value a condition compares a field's value against. Will have the type of the field
# or tuple[field_type]. The types match _COMPARABLE_FIELD_TYPES plus enums.
ConditionScalar = bool | int | float | str | datetime.date | Enum
ConditionValue = ConditionScalar | tuple[ConditionScalar, ...]
_CONDITION_SCALAR_TYPES: tuple[type, ...] = (*_COMPARABLE_FIELD_TYPES, Enum)


class ConditionOperator(Enum):
    """How a FieldCondition compares a field's value against the condition's value.
    GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN, and LESS_THAN_OR_EQUAL only
    compare numbers, and BEFORE, ON_OR_BEFORE, AFTER, ON_OR_AFTER, and
    BETWEEN_INCLUSIVE only compare dates."""

    EQUALS = "equals"
    NOT_EQUALS = "not_equals"
    GREATER_THAN = "greater_than"
    GREATER_THAN_OR_EQUAL = "greater_than_or_equal"
    LESS_THAN = "less_than"
    LESS_THAN_OR_EQUAL = "less_than_or_equal"
    IN = "in"
    NOT_IN = "not_in"
    BEFORE = "before"
    ON_OR_BEFORE = "on_or_before"
    AFTER = "after"
    ON_OR_AFTER = "on_or_after"
    BETWEEN_INCLUSIVE = "between_inclusive"


# Operators whose value is a list of values to test membership in.
_MEMBERSHIP_OPERATORS = {ConditionOperator.IN, ConditionOperator.NOT_IN}
# Operators whose value is a number to compare against.
_NUMERIC_OPERATORS = {
    ConditionOperator.GREATER_THAN,
    ConditionOperator.GREATER_THAN_OR_EQUAL,
    ConditionOperator.LESS_THAN,
    ConditionOperator.LESS_THAN_OR_EQUAL,
}
# Operators whose value is a single date to compare against.
_DATE_OPERATORS = {
    ConditionOperator.BEFORE,
    ConditionOperator.ON_OR_BEFORE,
    ConditionOperator.AFTER,
    ConditionOperator.ON_OR_AFTER,
}
# Operators that order values, which raise when the field has no value because a
# missing value can't be ordered.
_ORDERED_OPERATORS = (
    _NUMERIC_OPERATORS | _DATE_OPERATORS | {ConditionOperator.BETWEEN_INCLUSIVE}
)


def _tuple_if_list(value: Any) -> Any:
    """Returns value as a tuple if it is a list, and unchanged otherwise."""
    return tuple(value) if isinstance(value, list) else value


def _is_condition_value(instance: Any, attribute: attr.Attribute, value: Any) -> None:
    """Validator that ensures the field holds a ConditionValue."""
    items = value if isinstance(value, tuple) else (value,)
    if not all(isinstance(item, _CONDITION_SCALAR_TYPES) for item in items):
        raise TypeError(
            f"Field [{attribute.name}] on [{type(instance).__name__}] must be a "
            f"bool, int, float, str, date, or enum, or a tuple of them. Found "
            f"[{value!r}]."
        )


@attr.define(frozen=True, kw_only=True)
class FieldCondition:
    """A comparison of one field's value against a fixed value."""

    field: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Name of the field the condition reads, e.g. offense_category."""

    operator: ConditionOperator = attr.ib(
        validator=attr.validators.in_(ConditionOperator)
    )
    """How to compare the field's value against the value below."""

    value: ConditionValue = attr.ib(
        converter=_tuple_if_list, validator=_is_condition_value
    )
    """The value to compare against, already in the field's type: a tuple for IN
    and NOT_IN, a two-element tuple of dates for BETWEEN_INCLUSIVE, and a single
    value otherwise. A list is converted to a tuple for hashability."""

    def __attrs_post_init__(self) -> None:
        if self.operator in _MEMBERSHIP_OPERATORS and not (
            isinstance(self.value, tuple) and self.value
        ):
            raise ValueError(
                f"Condition on field [{self.field}] uses operator "
                f"[{self.operator.value}], which requires a non-empty tuple value. "
                f"Found [{self.value!r}]."
            )
        if self.operator is ConditionOperator.BETWEEN_INCLUSIVE and not (
            isinstance(self.value, tuple) and len(self.value) == 2
        ):
            raise ValueError(
                f"Condition on field [{self.field}] uses operator "
                f"[{self.operator.value}], which requires a two-element tuple. Found "
                f"[{self.value!r}]."
            )

    def is_met(self, get_value: Callable[[str], Any]) -> bool:
        """Returns whether the value get_value returns for this condition's field
        satisfies the condition. A missing value never satisfies EQUALS or IN,
        always satisfies NOT_EQUALS and NOT_IN, and raises for the operators that
        order values."""
        # TODO(NV-39): Validate sentence fields before checking any conditions.
        # NOT_IN shouldn't match a sentence just because its offense_id is missing.
        field_value = get_value(self.field)
        if field_value is None and self.operator in _ORDERED_OPERATORS:
            raise ValueError(
                f"Condition compares field [{self.field}] with operator "
                f"[{self.operator.value}], but the field has no value."
            )
        if self.operator is ConditionOperator.EQUALS:
            return field_value == self.value
        if self.operator is ConditionOperator.NOT_EQUALS:
            return field_value != self.value
        if self.operator is ConditionOperator.GREATER_THAN:
            return field_value > self.value
        if self.operator is ConditionOperator.GREATER_THAN_OR_EQUAL:
            return field_value >= self.value
        if self.operator is ConditionOperator.LESS_THAN:
            return field_value < self.value
        if self.operator is ConditionOperator.LESS_THAN_OR_EQUAL:
            return field_value <= self.value
        if self.operator is ConditionOperator.IN:
            return field_value in assert_type(self.value, tuple)
        if self.operator is ConditionOperator.NOT_IN:
            return field_value not in assert_type(self.value, tuple)
        if self.operator is ConditionOperator.BEFORE:
            return field_value < self.value
        if self.operator is ConditionOperator.ON_OR_BEFORE:
            return field_value <= self.value
        if self.operator is ConditionOperator.AFTER:
            return field_value > self.value
        if self.operator is ConditionOperator.ON_OR_AFTER:
            return field_value >= self.value
        if self.operator is ConditionOperator.BETWEEN_INCLUSIVE:
            start, end = assert_type(self.value, tuple)
            return start <= field_value <= end
        raise ValueError(f"Unexpected condition operator: [{self.operator}]")


def _condition_tuple(conditions: Iterable["Condition"]) -> tuple["Condition", ...]:
    """Returns conditions as a tuple."""
    return tuple(conditions)


@attr.define(frozen=True, kw_only=True)
class AllOf:
    """Conditions that must all hold."""

    conditions: tuple["Condition", ...] = attr.ib(
        converter=_condition_tuple, validator=attr_validators.is_non_empty_tuple
    )
    """The conditions, each a FieldCondition, AllOf, or AnyOf."""

    def is_met(self, get_value: Callable[[str], Any]) -> bool:
        """Returns whether every condition holds for the values get_value returns.
        Checks every condition even after one fails, so a condition that raises
        on a missing value raises wherever it appears in the list."""
        results = [condition.is_met(get_value) for condition in self.conditions]
        return all(results)


@attr.define(frozen=True, kw_only=True)
class AnyOf:
    """Conditions of which at least one must hold."""

    conditions: tuple["Condition", ...] = attr.ib(
        converter=_condition_tuple, validator=attr_validators.is_non_empty_tuple
    )
    """The conditions, each a FieldCondition, AllOf, or AnyOf."""

    def is_met(self, get_value: Callable[[str], Any]) -> bool:
        """Returns whether some condition holds for the values get_value returns.
        Checks every condition even after one holds, so a condition that raises
        on a missing value raises wherever it appears in the list."""
        results = [condition.is_met(get_value) for condition in self.conditions]
        return any(results)


Condition = FieldCondition | AllOf | AnyOf
# Validators for an attrs field holding a Condition, and one holding a Condition
# or None.
is_condition = attr.validators.instance_of(Condition)
is_opt_condition = attr.validators.optional(is_condition)


def sentence_condition_field_types(sentence_class: type[Sentence]) -> dict[str, type]:
    """Returns the type of each field of sentence_class that conditions can
    compare against, keyed by field name. A field that may be None maps to its
    non-None type (since the YAML won't be declaring a None field value).
    Fields of other types (including generic types like list[str], type unions
    like int | str, and datetime) are left out."""
    type_hints = typing.get_type_hints(sentence_class)
    field_types = {}
    for field in attr.fields(sentence_class):
        field_type = _non_none_type(type_hints[field.name])
        if not isinstance(field_type, type):
            continue
        # A datetime is also a date, but condition values are parsed as dates,
        # and a date never equals a datetime or orders against one.
        if issubclass(field_type, datetime.datetime):
            continue
        if issubclass(field_type, Enum) or issubclass(
            field_type, _COMPARABLE_FIELD_TYPES
        ):
            field_types[field.name] = field_type
    return field_types


def _non_none_type(type_hint: Any) -> Any:
    """Returns the one type besides None in type_hint if it is a union of that
    type and None, and type_hint unchanged otherwise."""
    if isinstance(type_hint, types.UnionType) or typing.get_origin(type_hint) is Union:
        non_none_types = [
            t for t in typing.get_args(type_hint) if t is not types.NoneType
        ]
        if len(non_none_types) == 1:
            return non_none_types[0]
    return type_hint


class ConditionParser:
    """Parses 'when' mappings into Conditions, checking each field and value
    against the fields the conditions can compare."""

    def __init__(
        self,
        *,
        field_types: Mapping[str, type],
        named_conditions: Mapping[str, Condition] | None,
    ) -> None:
        """
        Args:
            field_types: The type of each field the conditions can compare, keyed
                by field name.
            named_conditions: The conditions a 'when' mapping can refer to with
                condition: <name>, or None if these conditions can't refer to
                named conditions.
        """
        self._field_types = field_types
        self._named_conditions = named_conditions

    def parse(self, when_dict: YAMLDict, *, location: str) -> Condition:
        """Returns the condition when_dict defines, the mapping under a rule's when
        key. An error parsing it is raised again with location, where when_dict
        appears (e.g. term_multipliers entry [standard_term_reduction]), at the
        start of its message.

        Parsing pops every key from when_dict, so a mapping can only be parsed
        once. A YAML alias that reuses another rule's when mapping fails as empty;
        to share a condition between rules, define it as a named condition and
        refer to it with condition: <name>."""
        try:
            return self._parse(when_dict)
        except ValueError as e:
            raise ValueError(f"Invalid conditions in {location}. {e}") from e

    def _parse(self, when_dict: YAMLDict) -> Condition:
        """Returns the condition when_dict defines. when_dict is the mapping under
        a rule's when key, or one entry of an all_of or any_of list."""
        if not when_dict:
            raise ValueError("The conditions are empty.")
        conditions: list[Condition] = []
        for key in when_dict.keys():
            if key == ALL_OF_KEY:
                conditions.append(
                    AllOf(conditions=self._parse_list(when_dict, key=ALL_OF_KEY))
                )
            elif key == ANY_OF_KEY:
                conditions.append(
                    AnyOf(conditions=self._parse_list(when_dict, key=ANY_OF_KEY))
                )
            elif key == NAMED_CONDITION_KEY:
                conditions.append(
                    self._named_condition(when_dict.pop(NAMED_CONDITION_KEY, str))
                )
            else:
                conditions.append(
                    self._field_condition(
                        field=key,
                        raw_yaml_value=when_dict.pop_optional(key, object),
                    )
                )
        if len(conditions) == 1:
            return conditions[0]
        return AllOf(conditions=conditions)

    def _parse_list(self, when_dict: YAMLDict, *, key: str) -> list[Condition]:
        """Returns the conditions in the list when_dict holds under key, which
        is all_of or any_of."""
        conditions = [
            self._parse(condition_dict) for condition_dict in when_dict.pop_dicts(key)
        ]
        if not conditions:
            raise ValueError(f"The [{key}] list is empty.")
        return conditions

    def _named_condition(self, name: str) -> Condition:
        if self._named_conditions is None:
            raise ValueError(
                f"They refer to the named condition [{name}], but named conditions "
                f"can only be used in conditions on sentence fields."
            )
        if name not in self._named_conditions:
            raise ValueError(
                f"They refer to the named condition [{name}], which is not defined. "
                f"Defined conditions: {sorted(self._named_conditions)}."
            )
        return self._named_conditions[name]

    def _field_condition(self, *, field: str, raw_yaml_value: Any) -> FieldCondition:
        """Returns the condition that raw_yaml_value describes on a field.

        raw_yaml_value is the value set on the field in the YAML: a single value
        for equals, a list for in, or a one-key mapping from an operator to its
        operand, e.g. {"greater_than": 1000}.
        """
        if field not in self._field_types:
            raise ValueError(
                f"They use the field [{field}], which conditions here can't "
                f"compare. Fields they can compare: {sorted(self._field_types)}."
            )
        field_type = self._field_types[field]
        if raw_yaml_value is None:
            raise ValueError(
                f"The condition on field [{field}] has no value. Set "
                f"it to a value, a list of values, or a one-key mapping naming an "
                f"operator."
            )
        if isinstance(raw_yaml_value, list):
            operator = ConditionOperator.IN
            raw_value: Any = raw_yaml_value
        elif isinstance(raw_yaml_value, dict):
            if len(raw_yaml_value) != 1:
                raise ValueError(
                    f"The condition on field [{field}] must name "
                    f"exactly one operator. Found {sorted(raw_yaml_value)}."
                )
            [(operator_str, raw_value)] = raw_yaml_value.items()
            try:
                operator = ConditionOperator(operator_str)
            except ValueError as e:
                raise ValueError(
                    f"The condition on field [{field}] uses unknown "
                    f"operator [{operator_str}]. Operators: "
                    f"{[o.value for o in ConditionOperator]}."
                ) from e
        else:
            operator = ConditionOperator.EQUALS
            raw_value = raw_yaml_value

        def parse_value(value: Any) -> Any:
            return _parse_value(value=value, field=field, field_type=field_type)

        if operator in _MEMBERSHIP_OPERATORS:
            if not isinstance(raw_value, list) or not raw_value:
                raise ValueError(
                    f"The condition on field [{field}] uses operator "
                    f"[{operator.value}], which requires a non-empty list. Found "
                    f"[{raw_value!r}]."
                )
            value: Any = [parse_value(v) for v in raw_value]
        elif operator in _ORDERED_OPERATORS:
            self._check_ordered_field(field=field, operator=operator)
            if operator is ConditionOperator.BETWEEN_INCLUSIVE:
                if not isinstance(raw_value, list) or len(raw_value) != 2:
                    raise ValueError(
                        f"The condition on field [{field}] uses "
                        f"operator [{operator.value}], which requires a two-element "
                        f"list. Found [{raw_value!r}]."
                    )
                start, end = [parse_value(v) for v in raw_value]
                if start > end:
                    raise ValueError(
                        f"The condition on field [{field}] uses "
                        f"operator [{operator.value}] with start [{start}] after "
                        f"end [{end}], so it can never hold. List the earlier date "
                        f"first."
                    )
                value = [start, end]
            else:
                value = parse_value(raw_value)
        else:
            value = parse_value(raw_value)
        return FieldCondition(field=field, operator=operator, value=value)

    def _check_ordered_field(self, *, field: str, operator: ConditionOperator) -> None:
        field_type = self._field_types[field]
        is_numeric = issubclass(field_type, (int, float)) and field_type is not bool
        is_date = issubclass(field_type, datetime.date)
        if operator in _NUMERIC_OPERATORS and not is_numeric:
            raise ValueError(
                f"The condition on field [{field}] uses operator "
                f"[{operator.value}], which only compares numbers, but the field is "
                f"a [{field_type.__name__}]."
            )
        if operator not in _NUMERIC_OPERATORS and not is_date:
            raise ValueError(
                f"The condition on field [{field}] uses operator "
                f"[{operator.value}], which only compares dates, but the field is a "
                f"[{field_type.__name__}]."
            )


def _parse_value(*, value: Any, field: str, field_type: type) -> Any:
    """Returns value converted to field_type if possible, raises if not."""
    if issubclass(field_type, Enum):
        try:
            return field_type(value)
        except ValueError as e:
            raise ValueError(
                f"The condition on field [{field}] has value "
                f"[{value!r}], which the field can't take. Values it can take: "
                f"{[member.value for member in field_type]}."
            ) from e
    if issubclass(field_type, datetime.date):
        # YAML parses an unquoted value like 2007-07-01 00:00:00 into a datetime.
        if isinstance(value, datetime.datetime):
            raise _date_with_time_error(
                field=field,
                written_value=value.isoformat(sep=" "),
                date_value=value.date(),
            )
        if isinstance(value, datetime.date):
            return value
        if isinstance(value, str):
            try:
                return datetime.date.fromisoformat(value)
            except ValueError as e:
                if (datetime_value := _parse_iso_datetime(value)) is not None:
                    raise _date_with_time_error(
                        field=field,
                        written_value=value,
                        date_value=datetime_value.date(),
                    ) from e
                raise ValueError(
                    f"The condition on field [{field}] has value "
                    f"[{value}], which is not an ISO date."
                ) from e
    elif field_type is bool:
        if isinstance(value, bool):
            return value
    elif field_type is int:
        if isinstance(value, int) and not isinstance(value, bool):
            return value
    elif field_type is float:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return float(value)
    elif field_type is str:
        if isinstance(value, str):
            return value
    else:
        raise ValueError(f"Unexpected condition field type: [{field_type}]")
    raise ValueError(
        f"The condition on field [{field}] has value [{value!r}], "
        f"but the field is a [{field_type.__name__}]."
    )


def _parse_iso_datetime(value: str) -> datetime.datetime | None:
    """Returns value parsed as an ISO datetime, or None if it isn't one."""
    try:
        return datetime.datetime.fromisoformat(value)
    except ValueError:
        # fromisoformat raises ValueError only for a string that isn't an ISO
        # datetime, which the caller reports as not an ISO date.
        return None


def _date_with_time_error(
    *, field: str, written_value: str, date_value: datetime.date
) -> ValueError:
    """Returns the error for a date field whose condition value includes a time.

    Args:
        field: The field the condition compares.
        written_value: The value as the YAML wrote it, e.g. 2007-07-01 00:00:00.
        date_value: The date part of the value, suggested as the fix.
    """
    return ValueError(
        f"The condition on field [{field}] has value "
        f"[{written_value}], which includes a time, but the field is a date. Write "
        f"the date without a time, e.g. [{date_value.isoformat()}]."
    )


def parse_named_conditions(
    conditions_dict: YAMLDict,
    *,
    field_types: Mapping[str, type],
    inherited_conditions: Mapping[str, Condition],
) -> dict[str, Condition]:
    """Returns inherited_conditions plus each condition conditions_dict defines,
    keyed by name. A condition may refer to the inherited conditions and to the
    others in conditions_dict.

    Args:
        conditions_dict: The named conditions to parse, keyed by name.
        field_types: The type of each sentence field the conditions can compare,
            keyed by field name.
        inherited_conditions: Already parsed conditions new conditions can refer to,
            e.g. a state's shared conditions when parsing a regime's.
    """
    raw_condition_dicts = {
        name: conditions_dict.pop_dict(name) for name in conditions_dict.keys()
    }
    redefined_names = sorted(set(raw_condition_dicts) & set(inherited_conditions))
    if redefined_names:
        raise ValueError(
            f"The conditions {redefined_names} are already defined in the state's "
            f"shared conditions. Rename them or use the shared definitions."
        )
    named_conditions = _NamedConditions(
        raw_condition_dicts=raw_condition_dicts,
        inherited_conditions=inherited_conditions,
        field_types=field_types,
    )
    return {name: named_conditions[name] for name in named_conditions}


class _NamedConditions(Mapping[str, Condition]):
    """Named conditions are parsed the first time they're looked up, so a
    condition can refer to one defined after it."""

    def __init__(
        self,
        *,
        raw_condition_dicts: dict[str, YAMLDict],
        inherited_conditions: Mapping[str, Condition],
        field_types: Mapping[str, type],
    ) -> None:
        self._raw_condition_dicts = raw_condition_dicts
        self._parsed_conditions = dict(inherited_conditions)
        self._names_being_parsed: list[str] = []
        self._parser = ConditionParser(field_types=field_types, named_conditions=self)

    def __getitem__(self, name: str) -> Condition:
        if name in self._parsed_conditions:
            return self._parsed_conditions[name]
        if name not in self._raw_condition_dicts:
            raise KeyError(name)
        if name in self._names_being_parsed:
            cycle = self._names_being_parsed[self._names_being_parsed.index(name) :]
            raise ValueError(
                f"Named conditions refer to each other in a cycle: "
                f"[{' -> '.join([*cycle, name])}]."
            )
        self._names_being_parsed.append(name)
        self._parsed_conditions[name] = self._parser.parse(
            self._raw_condition_dicts[name], location=f"condition [{name}]"
        )
        self._names_being_parsed.pop()
        return self._parsed_conditions[name]

    def __iter__(self) -> typing.Iterator[str]:
        return iter(
            dict.fromkeys([*self._parsed_conditions, *self._raw_condition_dicts])
        )

    def __len__(self) -> int:
        return len({*self._parsed_conditions, *self._raw_condition_dicts})
