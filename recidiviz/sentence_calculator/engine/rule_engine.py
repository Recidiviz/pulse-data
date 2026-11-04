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
"""Rule engine that evaluates a law regime's rules against sentences."""

from enum import Enum

from recidiviz.sentence_calculator.models.conditions import Condition, FieldCondition
from recidiviz.sentence_calculator.models.inputs import Sentence
from recidiviz.sentence_calculator.models.outputs import TermMultiplier
from recidiviz.sentence_calculator.models.rules import (
    LawRegime,
    TermMultiplierRule,
    TermType,
)
from recidiviz.sentence_calculator.models.tracked import TrackedValue


def sentence_meets_condition(
    condition: Condition, sentence: Sentence, *, location: str
) -> bool:
    """Returns whether sentence's fields satisfy condition. An error checking
    condition is raised again with location, where condition appears (e.g.
    term_multipliers entry [standard_term_reduction]), in its message."""
    try:
        return condition.is_met(lambda field: getattr(sentence, field))
    except ValueError as e:
        raise ValueError(
            f"Could not check the conditions in {location} against sentence "
            f"[{sentence.sentence_id}]: {e}"
        ) from e


def _term_multiplier_location(rule: TermMultiplierRule) -> str:
    """Returns where rule appears in its regime, for error messages, e.g.
    term_multipliers entry [standard_term_reduction]."""
    return f"term_multipliers entry [{rule.name}]"


def _condition_field_names(condition: Condition) -> list[str]:
    """Returns the names of the fields condition reads, in the order they first
    appear in it."""
    if isinstance(condition, FieldCondition):
        return [condition.field]
    return list(
        dict.fromkeys(
            field
            for sub_condition in condition.conditions
            for field in _condition_field_names(sub_condition)
        )
    )


def _condition_field_values(condition: Condition, sentence: Sentence) -> str:
    """Returns the values sentence has for the fields condition reads, e.g.
    "offense_group=VIOLENCE, offense_category=C". Enums show their value, the
    form law regime files write them in."""
    field_values = []
    for field in _condition_field_names(condition):
        value = getattr(sentence, field)
        field_values.append(
            f"{field}={value.value if isinstance(value, Enum) else value}"
        )
    return ", ".join(field_values)


def _reduction_reaches_term(
    *, rule: TermMultiplierRule, term: TermType, sentence: Sentence
) -> tuple[bool, str]:
    """Returns whether rule's reduction comes off sentence's term, and why, e.g.
    (False, "applies_to min_term doesn't match offense_group=VIOLENCE")."""
    target = next((target for target in rule.applies_to if target.term is term), None)
    if target is None:
        return False, f"applies_to doesn't list {term.value}"
    if target.when is None:
        return True, f"applies_to lists {term.value} with no conditions"
    reaches_term = sentence_meets_condition(
        target.when,
        sentence,
        location=f"{_term_multiplier_location(rule)} applies_to [{term.value}]",
    )
    match_description = "matches" if reaches_term else "doesn't match"
    return reaches_term, (
        f"applies_to {term.value} {match_description} "
        f"{_condition_field_values(target.when, sentence)}"
    )


class RuleEngine:
    """Evaluates a law regime's rules against sentences.

    For each kind of rule, the first rule in the regime's list whose conditions
    the sentence meets applies.
    """

    def __init__(self, regime: LawRegime) -> None:
        self.regime = regime

    @staticmethod
    def _first_matching_rule(
        rules: list[TermMultiplierRule], sentence: Sentence
    ) -> TermMultiplierRule | None:
        return next(
            (
                rule
                for rule in rules
                if rule.when is None
                or sentence_meets_condition(
                    rule.when, sentence, location=_term_multiplier_location(rule)
                )
            ),
            None,
        )

    def get_term_multiplier(self, sentence: Sentence) -> TermMultiplier:
        """Returns the term_multipliers reduction for sentence, or no reduction
        (a multiplier of 1.0 that reduces no terms) if the regime has none. The
        multiplier's formula gives the sentence field values that decided which
        rule applies and which terms the rule's applies_to lets the reduction
        come off, e.g.

            multiplier=0.65 because standard_term_reduction applies to every
            sentence; doesn't reduce min_term because applies_to min_term
            doesn't match offense_group=VIOLENCE; reduces max_term because
            applies_to lists max_term with no conditions
        """
        rule = self._first_matching_rule(self.regime.term_multipliers, sentence)
        if rule is None:
            return TermMultiplier(
                multiplier=TrackedValue(
                    value=1.0,
                    label=f"{sentence.sentence_id}.term_multiplier",
                    formula="No term_multipliers rule; default 1.0",
                ),
                reduced_terms=frozenset(),
            )
        rule_reason = (
            f"{rule.name} applies to every sentence"
            if rule.when is None
            else f"{rule.name} matches {_condition_field_values(rule.when, sentence)}"
        )
        reduced_terms = set()
        term_reasons = []
        for term in TermType:
            reaches_term, term_reason = _reduction_reaches_term(
                rule=rule, term=term, sentence=sentence
            )
            if reaches_term:
                reduced_terms.add(term)
            reduction_description = "reduces" if reaches_term else "doesn't reduce"
            term_reasons.append(
                f"{reduction_description} {term.value} because {term_reason}"
            )
        return TermMultiplier(
            multiplier=TrackedValue(
                value=rule.multiplier,
                label=f"{sentence.sentence_id}.term_multiplier",
                rule_name=rule.name,
                formula="; ".join(
                    [f"multiplier={rule.multiplier} because {rule_reason}"]
                    + term_reasons
                ),
            ),
            reduced_terms=frozenset(reduced_terms),
        )
