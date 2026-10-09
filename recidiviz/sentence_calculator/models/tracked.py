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
"""TrackedValue models for sentence calculator.

TrackedValue provides full data lineage tracking for all calculated values.
Every computed value knows exactly which inputs and rules produced it,
enabling "show your work" derivation trees for each output.
"""

from datetime import date
from typing import Any, Generic, TypeVar

import attr

from recidiviz.common import attr_validators

# T_co is covariant so that a TrackedValue[int] is usable wherever a
# TrackedValue[int | date] is expected.
T_co = TypeVar("T_co", covariant=True)

# Covariant variables cannot be used as parameters. This is the
# equivalent of the above type for parameters. The split here should be
# opaque to consumers, it's just a hack to appease the type system.
ValueT = TypeVar("ValueT")


def _is_allowed_tracked_value(
    instance: Any, attribute: attr.Attribute, value: Any
) -> None:
    """Validates the `value` field of a TrackedValue.

    T_co is erased at runtime, so this can't check against the type parameter
    itself. Instead it checks against the concrete types every TrackedValue in
    this domain actually holds: a count of days, a multiplier, or a date --
    the types tracked_sum etc do arithmetic on and to_derivation_tree knows
    how to serialize.
    """
    if isinstance(value, date):
        return
    attr_validators.is_numerical_strict(instance, attribute, value)


@attr.define(frozen=True, kw_only=True)
class TrackedValue(Generic[T_co]):
    """A value with full derivation history.

    TrackedValue wraps any value and tracks its complete provenance back to
    original inputs. This enables automatic derivation trees without manual
    trace logging.

    Attributes:
        value: The current computed value
        label: Human-readable name (e.g., "S1.adjusted_min_days")
        sources: Parent TrackedValues this was derived from
        rule_name: Name of rule that produced this value (if any)
        formula: Human-readable formula showing the calculation
    """

    value: T_co = attr.ib(validator=_is_allowed_tracked_value)
    label: str = attr.ib(validator=attr_validators.is_non_empty_str)
    sources: tuple["TrackedValue[Any]", ...] = attr.ib(factory=tuple)
    rule_name: str | None = attr.ib(default=None, validator=attr_validators.is_opt_str)
    formula: str | None = attr.ib(default=None, validator=attr_validators.is_opt_str)

    @sources.validator
    def _check_sources(
        self, attribute: attr.Attribute, value: tuple["TrackedValue[Any]", ...]
    ) -> None:
        # TrackedValue can't be named directly in a validator built while the
        # class itself is still being defined, so this defers the lookup of
        # the class name to call time via is_tuple_of's closure instead.
        attr_validators.is_tuple_of(TrackedValue)(self, attribute, value)

    @staticmethod
    def input(value: ValueT, label: str) -> "TrackedValue[ValueT]":
        """Create a TrackedValue from user input (leaf node).

        Args:
            value: The input value
            label: Human-readable label for this input

        Returns:
            TrackedValue with no sources (leaf node in derivation tree)
        """
        return TrackedValue(
            value=value,
            label=label,
            sources=tuple(),
            rule_name=None,
            formula=None,
        )

    def derive(
        self,
        new_value: ValueT,
        label: str,
        formula: str,
        other_sources: list["TrackedValue[Any]"] | None = None,
        rule_name: str | None = None,
    ) -> "TrackedValue[ValueT]":
        """Derive a new TrackedValue from this one.

        Args:
            new_value: The computed value
            label: Human-readable label for the new value
            formula: Human-readable formula showing the calculation
            other_sources: Additional TrackedValues that contributed to this derivation
            rule_name: Name of the rule that produced this value

        Returns:
            New TrackedValue with this value (and other_sources) as sources
        """
        all_sources = [self]
        if other_sources:
            all_sources.extend(other_sources)

        return TrackedValue(
            value=new_value,
            label=label,
            sources=tuple(all_sources),
            rule_name=rule_name,
            formula=formula,
        )

    def to_derivation_tree(self) -> dict[str, Any]:
        """Serialize to nested dict for JSON/UI.

        Returns:
            Dictionary representation with nested sources forming a tree structure
        """

        def serialize_value(value: Any) -> Any:
            """Convert value to JSON-serializable format."""
            if isinstance(value, date):
                return str(value)
            return value

        result: dict[str, Any] = {
            "label": self.label,
            "value": serialize_value(self.value),
        }

        if self.formula:
            result["formula"] = self.formula

        if self.rule_name:
            result["rule"] = self.rule_name

        if self.sources:
            result["derived_from"] = [
                source.to_derivation_tree() for source in self.sources
            ]

        return result


def tracked_sum(values: list[TrackedValue[int]], label: str) -> TrackedValue[int]:
    """Sum a list of TrackedValues.

    Args:
        values: List of TrackedValues to sum
        label: Label for the resulting sum

    Returns:
        TrackedValue containing the sum with all input values as sources
    """
    if not values:
        return TrackedValue.input(0, label)

    total = sum(v.value for v in values)
    value_labels = [v.label for v in values]
    formula = " + ".join(value_labels)

    return TrackedValue(
        value=total,
        label=label,
        sources=tuple(values),
        rule_name=None,
        formula=formula,
    )
