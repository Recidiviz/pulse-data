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
"""Models for a law regime: the rules the sentence calculator applies to
sentences governed by one sentencing law.

A regime is defined in a YAML file under recidiviz/sentence_calculator/rules/
and takes its name from the file's name, uppercased. For example,
rules/us_nv/sb_413.yaml might hold:

    description: Nevada Senate Bill 413
    effective_date: "2027-07-01"
    use_calendar_term_conversion: true
    term_multipliers:
      - name: standard_term_reduction
        description: >-
          35% reduction of every max term, and of the min term outside A and B
          felonies
        multiplier: 0.65
        applies_to:
          max_term: always
          min_term:
            when:
              offense_category: { not_in: [A, B] }
    projected_dates:
      parole_eligibility_date:
        description: Eligible once the reduced minimum term is served
        formula: start_date + adjusted_min_days

...which parses into a LawRegime named SB_413 with one TermMultiplierRule, which
reduces every sentence's max term and reduces the min term of sentences outside
categories A and B, and one DateFormulaRule named parole_eligibility_date.

Each list of rules is checked in order, and the first rule whose 'when' conditions
hold applies. A rule without 'when' conditions applies to every sentence, so it
must come last. Each projected date has a single rule, which takes the date's
name.
"""

import ast
import datetime
import os
from collections.abc import Hashable, Mapping
from enum import Enum
from graphlib import CycleError, TopologicalSorter
from pathlib import Path
from typing import Any

import attr
import yaml

from recidiviz.common import attr_validators
from recidiviz.sentence_calculator import rules as rules_package
from recidiviz.sentence_calculator.models.conditions import (
    Condition,
    ConditionParser,
    is_opt_condition,
    parse_named_conditions,
    sentence_condition_field_types,
)
from recidiviz.sentence_calculator.models.inputs import Sentence
from recidiviz.utils.yaml_dict import YAMLDict
from recidiviz.utils.yaml_dict_validator import validate_yaml_matches_schema

_SCHEMA_DIR = os.path.join(os.path.dirname(rules_package.__file__), "schema")
LAW_REGIME_SCHEMA_PATH = os.path.join(_SCHEMA_DIR, "law_regime_schema.json")
STATE_CONDITIONS_SCHEMA_PATH = os.path.join(_SCHEMA_DIR, "state_conditions_schema.json")

# These defaults are slightly less than the true averages.
DEFAULT_DAYS_PER_MONTH = 30.0
DEFAULT_DAYS_PER_YEAR = 365.0

# The variable a credit cap's max_credit_days expression uses to refer to the
# length of the term being capped.
SENTENCE_LENGTH_DAYS_VARIABLE = "sentence_length_days"

# The values a projected date's formula and conditions can use.
# A formula can also use any other projected date.
PROJECTED_DATE_VALUE_TYPES: dict[str, type] = {
    # Date the first sentence on the controlling path starts.
    "start_date": datetime.date,
    # Sums over the controlling path of each sentence's original and adjusted terms.
    "unadjusted_min_days": int,
    "unadjusted_max_days": int,
    "adjusted_min_days": int,
    "adjusted_max_days": int,
    # Whole years in unadjusted_max_days, for statutes that set thresholds in
    # years. With calendar term conversion these are calendar years counted from
    # the RETRO date (start_date minus the first path sentence's jail credit),
    # where the terms themselves start counting, so a term that spans a leap day
    # isn't a year short.
    "unadjusted_max_years": int,
    # Sum of the jail credit on the controlling path.
    "jail_credit_days": int,
    # Days the regime's reduction took off the controlling path's max terms.
    "good_time_days": int,
    # Latest date any sentence, controlling or not, becomes eligible to go before
    # the parole board.
    "max_ped": datetime.date,
}

# The functions a rule expression can call to write a calendar duration, e.g.
# projected_expiration_date - months(12). Each takes a whole number, and a date
# plus or minus one moves by that many calendar years or months.
EXPRESSION_DURATION_FUNCTIONS = ("years", "months")

# The applies_to value for a term that a reduction comes off for every sentence
# the rule applies to.
APPLIES_TO_ALWAYS = "always"


class _UniqueKeySafeLoader(yaml.SafeLoader):
    """Loader that raises on a mapping with a repeated key. In a when mapping,
    that would silently drop a condition.

    It also raises on aliases (*name), including aliases merged in with <<. An
    alias makes its anchor's value one object shared between every place that
    uses it, and parsing a rule pops keys from that object, so every later use
    would find it empty. An empty when mapping parses as no conditions, so the
    rule would silently apply to every sentence."""

    def compose_node(self, parent: yaml.Node | None, index: int) -> yaml.Node | None:
        if self.check_event(yaml.AliasEvent):
            event = self.peek_event()
            mark = event.start_mark
            raise ValueError(
                f"The YAML file [{mark.name}] uses the alias [*{event.anchor}] at "
                f"line {mark.line + 1}, which law regime files don't support. Write "
                f"out the value in full. To share a condition between rules, "
                f"define it as a named condition and refer to it with "
                f"condition: <name>."
            )
        return super().compose_node(parent, index)

    def construct_mapping(
        self, node: yaml.MappingNode, deep: bool = False
    ) -> dict[Hashable, Any]:
        keys: set[Hashable] = set()
        for key_node, _ in node.value:
            key = self.construct_object(key_node, deep=deep)
            if key in keys:
                mark = key_node.start_mark
                raise ValueError(
                    f"The YAML file [{mark.name}] repeats the key [{key}] in one "
                    f"mapping, at line {mark.line + 1}. Remove one of them. To "
                    f"require several conditions on the same field, or several "
                    f"named conditions, list them under all_of."
                )
            keys.add(key)
        return super().construct_mapping(node, deep=deep)


def _yaml_dict_from_path(yaml_path: str | Path) -> YAMLDict:
    """Returns the top-level mapping of the YAML file at yaml_path, raising if
    any mapping in the file repeats a key or the file uses an alias."""
    with open(yaml_path, encoding="utf-8") as yaml_file:
        loader = _UniqueKeySafeLoader(yaml_file)
        try:
            loaded_yaml = loader.get_single_data()
        finally:
            loader.dispose()
    if not isinstance(loaded_yaml, dict):
        raise ValueError(
            f"Expected the YAML file [{yaml_path}] to hold a mapping. Found "
            f"[{type(loaded_yaml)}]."
        )
    return YAMLDict(loaded_yaml)


def _with_dates_as_iso_strings(value: Any) -> Any:
    """Returns value with each date in it, at any depth, replaced by its ISO
    string. Dates with a time of day are left as they are."""
    if isinstance(value, dict):
        return {key: _with_dates_as_iso_strings(v) for key, v in value.items()}
    if isinstance(value, list):
        return [_with_dates_as_iso_strings(v) for v in value]
    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
        return value.isoformat()
    return value


def _validate_against_schema(yaml_dict: YAMLDict, json_schema_path: str) -> None:
    """Raises if yaml_dict doesn't match the JSON schema at json_schema_path.

    YAML parses an unquoted date like 2027-07-01 into a date, but JSON schemas
    can only describe strings, so dates are checked as their ISO strings. A
    quoted and an unquoted date pass the same checks, and a date with a time of
    day fails the schema's string type."""
    validate_yaml_matches_schema(
        YAMLDict(_with_dates_as_iso_strings(yaml_dict.get())), json_schema_path
    )


def _pop_date(yaml_dict: YAMLDict, field: str) -> datetime.date:
    """Returns the 'date at' field, which YAML parses into a date when it is
    unquoted and leaves as an ISO string when it is quoted."""
    value = yaml_dict.pop(field, object)
    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.date.fromisoformat(value)
        except ValueError as e:
            raise ValueError(
                f"The field [{field}] must be an ISO date, e.g. 2027-07-01. Found "
                f"[{value}]."
            ) from e
    raise ValueError(f"The field [{field}] must be a date. Found [{value!r}].")


def _pop_optional_number(yaml_dict: YAMLDict, field: str) -> float | None:
    """Returns the 'number at' field as a float, or None if it is absent. YAML
    parses a whole number like 20 as an int, which is as valid as 20.0 here."""
    value = yaml_dict.pop_optional(field, object)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"The field [{field}] must be a number. Found [{value!r}].")
    return float(value)


def _pop_number(yaml_dict: YAMLDict, field: str) -> float:
    """Returns the 'number at' field as a float, raising if it is absent."""
    value = _pop_optional_number(yaml_dict, field)
    if value is None:
        raise ValueError(f"Missing required number [{field}].")
    return value


def _pop_when(
    yaml_dict: YAMLDict, *, parser: ConditionParser, location: str
) -> Condition | None:
    """Returns the condition in the rule's when mapping, or None if the rule has
    no when or it is empty, meaning the rule applies to every sentence. location
    names the rule in error messages."""
    when_dict = yaml_dict.pop_dict_optional("when")
    return parser.parse(when_dict, location=location) if when_dict else None


def _check_no_unexpected_keys(yaml_dict: YAMLDict, *, location: str) -> None:
    """Raises if any keys remain in yaml_dict after parsing popped every key it
    expects, which catches misspelled or unsupported fields. location names the
    rule or file yaml_dict came from in the error message."""
    if yaml_dict:
        raise ValueError(
            f"Found unexpected keys in {location}: {repr(yaml_dict.get())}"
        )


def expression_variable_names(*, expression: str, rule_name: str) -> set[str]:
    """Returns the names of the variables expression uses, raising if it is not
    a valid expression. A duration function such as months in months(12) isn't
    a variable."""
    try:
        tree = ast.parse(expression, mode="eval")
    except SyntaxError as e:
        raise ValueError(
            f"Expression [{expression}] in rule [{rule_name}] is not valid syntax."
        ) from e
    duration_function_nodes = {
        node.func
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id in EXPRESSION_DURATION_FUNCTIONS
    }
    return {
        node.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Name) and node not in duration_function_nodes
    }


class TermType(Enum):
    """A sentence term that a reduction can come off. The values match the
    Sentence fields that hold the terms."""

    MIN_TERM = "min_term"
    MAX_TERM = "max_term"


# The TermType values, as error messages list them, e.g. ['min_term', 'max_term'].
_TERM_TYPE_VALUES = [term.value for term in TermType]


@attr.define(frozen=True, kw_only=True)
class ReductionTarget:
    """A term that a reduction comes off, and the sentences it comes off for."""

    term: TermType = attr.ib(validator=attr.validators.in_(TermType))
    """The term the reduction comes off."""

    when: Condition | None = attr.ib(validator=is_opt_condition)
    """Conditions a sentence must meet for the reduction to come off this term,
    or None if it comes off this term for every sentence the rule applies to."""


def _pop_applies_to(
    yaml_dict: YAMLDict, *, parser: ConditionParser, rule_location: str
) -> list[ReductionTarget]:
    """Returns the ReductionTargets in a rule's applies_to mapping, which maps each
    term the reduction comes off to always, or to the when conditions a sentence
    must meet for the reduction to come off it, e.g.

        applies_to:
          max_term: always
          min_term:
            when:
              offense_category: { not_in: [A, B] }

    Args:
        yaml_dict: The rule, which holds the applies_to mapping.
        parser: Parses conditions on sentence fields.
        rule_location: Where the rule appears, e.g. term_multipliers entry
            [standard_term_reduction], for error messages.
    """
    applies_to_dict = yaml_dict.pop_dict("applies_to")
    targets = []
    for term_name in applies_to_dict.keys():
        term = TermType(term_name)
        location = f"{rule_location} applies_to [{term.value}]"
        if applies_to_dict.peek_type(term_name) is str:
            value = applies_to_dict.pop(term_name, str)
            if value != APPLIES_TO_ALWAYS:
                raise ValueError(
                    f"The value of {location} is [{value}]. Write "
                    f"[{APPLIES_TO_ALWAYS}], or a mapping with when conditions."
                )
            targets.append(ReductionTarget(term=term, when=None))
            continue
        target_dict = applies_to_dict.pop_dict(term_name)
        targets.append(
            ReductionTarget(
                term=term,
                when=parser.parse(target_dict.pop_dict("when"), location=location),
            )
        )
        _check_no_unexpected_keys(target_dict, location=location)
    return targets


def _check_applies_to_lists_each_term_once(
    *, rule_label: str, applies_to: list[ReductionTarget]
) -> None:
    """Raises if applies_to lists a term more than once. rule_label names the
    rule in the error, e.g. Term multiplier [standard_term_reduction]."""
    terms = [target.term.value for target in applies_to]
    duplicate_terms = sorted({term for term in terms if terms.count(term) > 1})
    if duplicate_terms:
        raise ValueError(
            f"{rule_label} lists the terms {duplicate_terms} more than once in "
            f"applies_to. List each term once, and combine its conditions with "
            f"any_of or all_of."
        )


@attr.define(frozen=True, kw_only=True)
class TermMultiplierRule:
    """A flat reduction applied to a sentence's terms at sentencing."""

    name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Identifier for the rule, unique within its regime."""

    description: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Plain-language explanation of what the rule does and why."""

    when: Condition | None = attr.ib(validator=is_opt_condition)
    """Conditions a sentence must meet for the rule to apply, or None if it
    applies to every sentence."""

    multiplier: float = attr.ib(validator=attr_validators.is_float_between_zero_and_one)
    """The fraction of each term left after the reduction, e.g. 0.65 for a 35%
    reduction."""

    applies_to: list[ReductionTarget] = attr.ib(
        validator=[
            attr_validators.is_non_empty_list,
            attr_validators.is_list_of(ReductionTarget),
        ]
    )
    """The terms the reduction comes off. The reduction never comes off a term
    that isn't listed."""

    statute: str | None = attr.ib(validator=attr_validators.is_opt_str)
    """Citation for the law the rule encodes, if any."""

    def __attrs_post_init__(self) -> None:
        _check_applies_to_lists_each_term_once(
            rule_label=f"Term multiplier [{self.name}]", applies_to=self.applies_to
        )

    @classmethod
    def from_yaml_dict(
        cls, yaml_dict: YAMLDict, *, parser: ConditionParser
    ) -> "TermMultiplierRule":
        """Returns the TermMultiplierRule defined by one term_multipliers entry."""
        name = yaml_dict.pop("name", str)
        location = f"term_multipliers entry [{name}]"
        rule = TermMultiplierRule(
            name=name,
            description=yaml_dict.pop("description", str),
            when=_pop_when(yaml_dict, parser=parser, location=location),
            multiplier=_pop_number(yaml_dict, "multiplier"),
            applies_to=_pop_applies_to(
                yaml_dict, parser=parser, rule_location=location
            ),
            statute=yaml_dict.pop_optional("statute", str),
        )
        _check_no_unexpected_keys(yaml_dict, location=location)
        return rule


@attr.define(frozen=True, kw_only=True)
class CreditRateRule:
    """Sentence-reducing credits a sentence earns for each month served."""

    name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Identifier for the rule, unique within its regime."""

    description: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Plain-language explanation of what the rule does and why."""

    when: Condition | None = attr.ib(validator=is_opt_condition)
    """Conditions a sentence must meet for the rule to apply, or None if it
    applies to every sentence."""

    rate: float = attr.ib(validator=attr_validators.is_non_negative_float)
    """Credit days earned per month served."""

    applies_to: list[ReductionTarget] = attr.ib(
        validator=[
            attr_validators.is_non_empty_list,
            attr_validators.is_list_of(ReductionTarget),
        ]
    )
    """The terms the credits come off. The credits never come off a term that
    isn't listed."""

    statute: str | None = attr.ib(validator=attr_validators.is_opt_str)
    """Citation for the law the rule encodes, if any."""

    def __attrs_post_init__(self) -> None:
        _check_applies_to_lists_each_term_once(
            rule_label=f"Credit rate [{self.name}]", applies_to=self.applies_to
        )

    @classmethod
    def from_yaml_dict(
        cls, yaml_dict: YAMLDict, *, parser: ConditionParser
    ) -> "CreditRateRule":
        """Returns the CreditRateRule defined by one credit_rates entry."""
        name = yaml_dict.pop("name", str)
        location = f"credit_rates entry [{name}]"
        rule = CreditRateRule(
            name=name,
            description=yaml_dict.pop("description", str),
            when=_pop_when(yaml_dict, parser=parser, location=location),
            rate=_pop_number(yaml_dict, "rate"),
            applies_to=_pop_applies_to(
                yaml_dict, parser=parser, rule_location=location
            ),
            statute=yaml_dict.pop_optional("statute", str),
        )
        _check_no_unexpected_keys(yaml_dict, location=location)
        return rule


@attr.define(frozen=True, kw_only=True)
class CreditCapRule:
    """A ceiling on the credits a sentence can earn against its terms."""

    name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Identifier for the rule, unique within its regime."""

    description: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Plain-language explanation of what the rule does and why."""

    when: Condition | None = attr.ib(validator=is_opt_condition)
    """Conditions a sentence must meet for the rule to apply, or None if it
    applies to every sentence."""

    max_credit_days: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """An arithmetic expression over sentence_length_days, the length of the term
    being capped, e.g. "sentence_length_days * 0.58"."""

    binds: frozenset[TermType] = attr.ib(
        validator=attr_validators.is_frozenset_of(TermType)
    )
    """The terms whose credits the cap limits. Credits against any other term
    are uncapped."""

    statute: str | None = attr.ib(validator=attr_validators.is_opt_str)
    """Citation for the law the rule encodes, if any."""

    def __attrs_post_init__(self) -> None:
        if not self.binds:
            raise ValueError(
                f"Credit cap [{self.name}] binds no terms. List one or more of "
                f"{_TERM_TYPE_VALUES} in binds."
            )
        unknown_names = expression_variable_names(
            expression=self.max_credit_days, rule_name=self.name
        ) - {SENTENCE_LENGTH_DAYS_VARIABLE}
        if unknown_names:
            raise ValueError(
                f"Credit cap [{self.name}] uses {sorted(unknown_names)} in "
                f"max_credit_days, but it can only use "
                f"[{SENTENCE_LENGTH_DAYS_VARIABLE}]."
            )

    @classmethod
    def from_yaml_dict(
        cls, yaml_dict: YAMLDict, *, parser: ConditionParser
    ) -> "CreditCapRule":
        """Returns the CreditCapRule defined by one credit_caps entry."""
        name = yaml_dict.pop("name", str)
        location = f"credit_caps entry [{name}]"
        rule = CreditCapRule(
            name=name,
            description=yaml_dict.pop("description", str),
            when=_pop_when(yaml_dict, parser=parser, location=location),
            max_credit_days=yaml_dict.pop("max_credit_days", str),
            binds=frozenset(
                TermType(term) for term in yaml_dict.pop_list("binds", str)
            ),
            statute=yaml_dict.pop_optional("statute", str),
        )
        _check_no_unexpected_keys(yaml_dict, location=location)
        return rule


@attr.define(frozen=True, kw_only=True)
class DateFormulaRule:
    """A formula that computes one projected date (e.g. a parole eligibility
    date) from the calculation's values and other projected dates."""

    name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Identifier for the rule, unique within its regime."""

    description: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Plain-language explanation of what the rule does and why."""

    when: Condition | None = attr.ib(validator=is_opt_condition)
    """Conditions on the values in PROJECTED_DATE_VALUE_TYPES that must hold for
    the date to be computed, or None if it is always computed."""

    formula: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """The expression that computes the date, e.g. "start_date + adjusted_min_days"."""

    statute: str | None = attr.ib(validator=attr_validators.is_opt_str)
    """Citation for the law the rule encodes, if any."""

    @classmethod
    def from_yaml_dict(
        cls, yaml_dict: YAMLDict, *, date_name: str, parser: ConditionParser
    ) -> "DateFormulaRule":
        """Returns the DateFormulaRule defined for the projected date date_name.
        The rule takes the date's name.

        Args:
            yaml_dict: The rule.
            date_name: The projected date the rule computes.
            parser: Parses conditions on calculation values.
        """
        location = f"projected date [{date_name}]"
        rule = DateFormulaRule(
            name=date_name,
            description=yaml_dict.pop("description", str),
            when=_pop_when(yaml_dict, parser=parser, location=location),
            formula=yaml_dict.pop("formula", str),
            statute=yaml_dict.pop_optional("statute", str),
        )
        _check_no_unexpected_keys(yaml_dict, location=location)
        return rule


def _check_rule_order(
    *,
    regime_name: str,
    section: str,
    rules: list[TermMultiplierRule] | list[CreditRateRule] | list[CreditCapRule],
    requires_unconditional_last_rule: bool,
) -> None:
    """Raises if a rule without conditions, which applies to everything, comes
    before other rules in section, or if requires_unconditional_last_rule and the
    last rule has conditions."""
    for rule in rules[:-1]:
        if rule.when is None:
            raise ValueError(
                f"Law regime [{regime_name}] has rule [{rule.name}] in {section} "
                f"with no when conditions, so the rules after it never apply. Move "
                f"it to the end of {section}."
            )
    if requires_unconditional_last_rule and rules and rules[-1].when is not None:
        raise ValueError(
            f"Law regime [{regime_name}] has when conditions on [{rules[-1].name}], "
            f"the last rule in {section}, so sentences that match no rule get no "
            f"reduction. Add a last rule without when conditions for every other "
            f"sentence."
        )


@attr.define(frozen=True, kw_only=True)
class LawRegime:
    """A collection of rules for a specific law regime.

    A regime shortens sentences with term_multipliers or credit_rates, not both,
    or doesn't shorten them at all.
    """

    name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Identifier for the regime, e.g. SB_413: the name of its YAML file without
    the extension, uppercased."""

    description: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Human-readable explanation of the regime."""

    effective_date: datetime.date = attr.ib(validator=attr_validators.is_date)
    """Date the law this regime encodes took effect."""

    use_calendar_term_conversion: bool = attr.ib(validator=attr_validators.is_bool)
    """Whether to convert a term to days by counting the real calendar days from
    the sentence's start date, so a one year term starting 2024-01-01 is 366
    days, rather than with the flat days_per_year and days_per_month below."""

    days_per_month: float = attr.ib(validator=attr_validators.is_positive_float)
    """Days in a month for flat term conversion and for accruing monthly credits."""

    days_per_year: float = attr.ib(validator=attr_validators.is_positive_float)
    """Days in a year for flat term conversion."""

    term_multipliers: list[TermMultiplierRule] = attr.ib(
        validator=attr_validators.is_list_of(TermMultiplierRule)
    )
    """Flat reductions applied at sentencing. The first rule whose conditions a
    sentence meets applies."""

    credit_rates: list[CreditRateRule] = attr.ib(
        validator=attr_validators.is_list_of(CreditRateRule)
    )
    """Credits earned per month served. The first rule whose conditions a
    sentence meets applies."""

    # TODO(NV-54): Apply the most conservative cap that applies to a sentence or
    # group of sentences, rather than the first match, once a regime has more
    # than one cap.
    credit_caps: list[CreditCapRule] = attr.ib(
        validator=attr_validators.is_list_of(CreditCapRule)
    )
    """Ceilings on the credits from credit_rates. The first rule whose conditions
    a sentence meets applies, and a sentence that meets none is uncapped."""

    projected_dates: dict[str, DateFormulaRule] = attr.ib(
        validator=attr_validators.is_dict_of(str, DateFormulaRule)
    )
    """The rule for each projected date, keyed by the date's name, e.g.
    parole_eligibility_date. A date whose rule's conditions don't hold is left
    out of the calculation."""

    @property
    def all_rule_names(self) -> list[str]:
        """Returns the names of every rule in the regime."""
        return [
            *(rule.name for rule in self.term_multipliers),
            *(rule.name for rule in self.credit_rates),
            *(rule.name for rule in self.credit_caps),
            *(rule.name for rule in self.projected_dates.values()),
        ]

    @property
    def projected_date_evaluation_order(self) -> list[str]:
        """Returns the names of the projected dates ordered so that every date
        comes after the dates its formula uses, raising if the dates depend on
        each other in a cycle."""
        dependencies = {
            date_name: expression_variable_names(
                expression=rule.formula, rule_name=rule.name
            )
            & set(self.projected_dates)
            for date_name, rule in self.projected_dates.items()
        }
        try:
            return list(TopologicalSorter(dependencies).static_order())
        except CycleError as e:
            # CycleError lists each date before the date that uses it.
            cycle = list(reversed(e.args[1]))
            raise ValueError(
                f"Law regime [{self.name}] has projected dates that depend on each "
                f"other in a cycle, where each date's formula uses the next: "
                f"[{' -> '.join(cycle)}]. Change the formulas so no date depends on "
                f"itself."
            ) from e

    def __attrs_post_init__(self) -> None:
        if self.term_multipliers and self.credit_rates:
            raise ValueError(
                f"Law regime [{self.name}] has both term_multipliers and "
                f"credit_rates. A regime shortens sentences one way; remove one."
            )
        if self.credit_caps and not self.credit_rates:
            raise ValueError(
                f"Law regime [{self.name}] has credit_caps but no credit_rates, so "
                f"there are no credits to cap. Add credit_rates or remove "
                f"credit_caps."
            )
        _check_rule_order(
            regime_name=self.name,
            section="term_multipliers",
            rules=self.term_multipliers,
            requires_unconditional_last_rule=True,
        )
        _check_rule_order(
            regime_name=self.name,
            section="credit_rates",
            rules=self.credit_rates,
            requires_unconditional_last_rule=True,
        )
        _check_rule_order(
            regime_name=self.name,
            section="credit_caps",
            rules=self.credit_caps,
            requires_unconditional_last_rule=False,
        )
        for date_name, rule in self.projected_dates.items():
            self._check_projected_date(date_name=date_name, rule=rule)
        # Raises if the projected dates depend on each other in a cycle.
        _ = self.projected_date_evaluation_order
        rule_names = self.all_rule_names
        duplicate_names = sorted({n for n in rule_names if rule_names.count(n) > 1})
        if duplicate_names:
            raise ValueError(
                f"Law regime [{self.name}] has duplicate rule names: "
                f"{duplicate_names}."
            )

    def _check_projected_date(self, *, date_name: str, rule: DateFormulaRule) -> None:
        """Raises if date_name is the name of a calculation value, or if its
        rule's formula refers to something other than a calculation value or
        projected date."""
        if date_name in PROJECTED_DATE_VALUE_TYPES:
            raise ValueError(
                f"Law regime [{self.name}] has a projected date named "
                f"[{date_name}], which is already the name of a calculation value. "
                f"Rename the projected date."
            )
        available_names = {*PROJECTED_DATE_VALUE_TYPES, *self.projected_dates}
        # This only checks the names a formula uses. A formula with an
        # unsupported operator or a malformed duration, e.g. start_date ** 2 or
        # start_date + months(1.5), still loads and raises only when a
        # calculation evaluates it. Tests for each regime should compute its
        # projected dates so that errors like these fail there with a clear
        # message instead of at calculation time.
        unknown_names = (
            expression_variable_names(expression=rule.formula, rule_name=rule.name)
            - available_names
        )
        if unknown_names:
            raise ValueError(
                f"Rule [{rule.name}] uses {sorted(unknown_names)} in its formula, "
                f"which are neither calculation values nor projected dates. "
                f"Available names: {sorted(available_names)}."
            )

    @staticmethod
    def name_for_yaml_path(yaml_path: str | Path) -> str:
        """Returns the name of the regime defined in the YAML file at yaml_path,
        e.g. SB_413 for .../us_nv/sb_413.yaml."""
        return Path(yaml_path).stem.upper()

    @classmethod
    def from_yaml(
        cls,
        yaml_path: str | Path,
        *,
        sentence_class: type[Sentence],
        shared_conditions: Mapping[str, Condition],
    ) -> "LawRegime":
        """Returns the LawRegime defined in the YAML file at yaml_path, after
        checking the file against the regime JSON schema.

        Args:
            yaml_path: Path of the regime's YAML file, whose name gives the
                regime its name.
            sentence_class: The Sentence subclass for the regime's state, whose
                fields conditions on sentences can compare.
            shared_conditions: The state's named conditions, which the regime's
                conditions can refer to.
        """
        yaml_dict = _yaml_dict_from_path(yaml_path)
        _validate_against_schema(yaml_dict, LAW_REGIME_SCHEMA_PATH)

        sentence_field_types = sentence_condition_field_types(sentence_class)
        sentence_parser = ConditionParser(
            field_types=sentence_field_types,
            named_conditions=parse_named_conditions(
                yaml_dict.pop_dict_optional("conditions") or YAMLDict({}),
                field_types=sentence_field_types,
                inherited_conditions=shared_conditions,
            ),
        )
        values_parser = ConditionParser(
            field_types=PROJECTED_DATE_VALUE_TYPES, named_conditions=None
        )
        days_per_month = _pop_optional_number(yaml_dict, "days_per_month")
        days_per_year = _pop_optional_number(yaml_dict, "days_per_year")
        regime = LawRegime(
            name=cls.name_for_yaml_path(yaml_path),
            description=yaml_dict.pop("description", str),
            effective_date=_pop_date(yaml_dict, "effective_date"),
            use_calendar_term_conversion=yaml_dict.pop_optional(
                "use_calendar_term_conversion", bool
            )
            or False,
            days_per_month=days_per_month or DEFAULT_DAYS_PER_MONTH,
            days_per_year=days_per_year or DEFAULT_DAYS_PER_YEAR,
            term_multipliers=[
                TermMultiplierRule.from_yaml_dict(rule_dict, parser=sentence_parser)
                for rule_dict in yaml_dict.pop_dicts_optional("term_multipliers") or []
            ],
            credit_rates=[
                CreditRateRule.from_yaml_dict(rule_dict, parser=sentence_parser)
                for rule_dict in yaml_dict.pop_dicts_optional("credit_rates") or []
            ],
            credit_caps=[
                CreditCapRule.from_yaml_dict(rule_dict, parser=sentence_parser)
                for rule_dict in yaml_dict.pop_dicts_optional("credit_caps") or []
            ],
            projected_dates=_parse_projected_dates(
                yaml_dict.pop_dict_optional("projected_dates") or YAMLDict({}),
                parser=values_parser,
            ),
        )
        _check_no_unexpected_keys(yaml_dict, location=f"law regime file [{yaml_path}]")
        return regime


def _parse_projected_dates(
    projected_dates_dict: YAMLDict, *, parser: ConditionParser
) -> dict[str, DateFormulaRule]:
    """Returns the rule for each projected date in a regime's projected_dates.
    'parser' parses the rules' conditions on calculation values."""
    return {
        date_name: DateFormulaRule.from_yaml_dict(
            projected_dates_dict.pop_dict(date_name), date_name=date_name, parser=parser
        )
        for date_name in projected_dates_dict.keys()
    }


def parse_state_conditions(
    yaml_path: str | Path, *, sentence_class: type[Sentence]
) -> dict[str, Condition]:
    """Returns the named conditions in a state's shared conditions file, which
    every regime for the state can refer to, keyed by name. The file maps each
    name to a condition, e.g.

        felony_dui:
          offense_id: [1576, 1577]

    Args:
        yaml_path: Path of the state's conditions file.
        sentence_class: The Sentence subclass for the state, whose fields the
            conditions can compare.
    """
    yaml_dict = _yaml_dict_from_path(yaml_path)
    _validate_against_schema(yaml_dict, STATE_CONDITIONS_SCHEMA_PATH)
    return parse_named_conditions(
        yaml_dict,
        field_types=sentence_condition_field_types(sentence_class),
        inherited_conditions={},
    )
