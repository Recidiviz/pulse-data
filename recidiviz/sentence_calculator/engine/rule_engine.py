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

from collections.abc import Sequence
from enum import Enum
from typing import TypeVar

from recidiviz.sentence_calculator.engine.expression import evaluate_days_expression
from recidiviz.sentence_calculator.models.conditions import (
    Condition,
    condition_field_names,
)
from recidiviz.sentence_calculator.models.inputs import Sentence
from recidiviz.sentence_calculator.models.outputs import (
    CreditCap,
    ProjectedCreditRate,
    TermMultiplier,
)
from recidiviz.sentence_calculator.models.rules import (
    SENTENCE_LENGTH_DAYS_VARIABLE,
    CreditCapRule,
    CreditRateRule,
    CreditRateUnit,
    LawRegime,
    TermMultiplierRule,
    TermType,
)
from recidiviz.sentence_calculator.models.tracked import TrackedValue

_SentenceRuleT = TypeVar(
    "_SentenceRuleT", TermMultiplierRule, CreditRateRule, CreditCapRule
)


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


def _rule_location(rule: TermMultiplierRule | CreditRateRule | CreditCapRule) -> str:
    """Returns where rule appears in its regime, for error messages, e.g.
    term_multipliers entry [standard_term_reduction]."""
    if isinstance(rule, TermMultiplierRule):
        return f"term_multipliers entry [{rule.name}]"
    if isinstance(rule, CreditRateRule):
        return f"credit_rates entry [{rule.name}]"
    if isinstance(rule, CreditCapRule):
        return f"credit_caps entry [{rule.name}]"
    raise ValueError(f"Unexpected rule type: [{type(rule)}]")


def _condition_field_values(condition: Condition, sentence: Sentence) -> str:
    """Returns the values sentence has for the fields condition reads, e.g.
    "offense_group=VIOLENCE, offense_category=C". Enums show their value, the
    form law regime files write them in."""
    field_values = []
    for field in condition_field_names(condition):
        value = getattr(sentence, field)
        field_values.append(
            f"{field}={value.value if isinstance(value, Enum) else value}"
        )
    return ", ".join(field_values)


def _rule_match_reason(
    rule: TermMultiplierRule | CreditRateRule, sentence: Sentence
) -> str:
    """Returns why rule applies to sentence, e.g. "violent_term_reduction
    matches offense_group=VIOLENCE"."""
    if rule.when is None:
        return f"{rule.name} applies to every sentence"
    return f"{rule.name} matches {_condition_field_values(rule.when, sentence)}"


def _reduction_reaches_term(
    *, rule: TermMultiplierRule | CreditRateRule, term: TermType, sentence: Sentence
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
        location=f"{_rule_location(rule)} applies_to [{term.value}]",
    )
    match_description = "matches" if reaches_term else "doesn't match"
    return reaches_term, (
        f"applies_to {term.value} {match_description} "
        f"{_condition_field_values(target.when, sentence)}"
    )


def _reduced_terms(
    *, rule: TermMultiplierRule | CreditRateRule, sentence: Sentence
) -> tuple[frozenset[TermType], list[str]]:
    """Returns the terms rule's reduction comes off for sentence, and why for
    each term, e.g. (frozenset({TermType.MAX_TERM}), ["doesn't reduce min_term
    because applies_to min_term doesn't match offense_group=VIOLENCE", "reduces
    max_term because applies_to lists max_term with no conditions"])."""
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
    return frozenset(reduced_terms), term_reasons


class MissingSentenceFieldError(ValueError):
    """Raised when a sentence has no value for a field its law regime's
    conditions read. These errors come from the input, not the calculation, so
    callers can report them back to whoever supplied the sentences."""


class RuleEngine:
    """Evaluates a law regime's rules against sentences.

    For each kind of rule, the first rule in the regime's list whose conditions
    the sentence meets applies.
    """

    def __init__(self, regime: LawRegime) -> None:
        self.regime = regime

    def check_sentence_fields(self, sentences: Sequence[Sentence]) -> None:
        """Raises MissingSentenceFieldError if any of sentences has no value for a
        field the regime's conditions read."""
        field_names = self.regime.sentence_condition_field_names
        for sentence in sentences:
            missing_field_names = [
                field for field in field_names if getattr(sentence, field) is None
            ]
            if missing_field_names:
                raise MissingSentenceFieldError(
                    f"Sentence [{sentence.sentence_id}] has no value for "
                    f"{missing_field_names}, which law regime [{self.regime.name}] "
                    f"has conditions on. Provide a value for each."
                )

    @staticmethod
    def _first_matching_rule(
        rules: list[_SentenceRuleT], sentence: Sentence
    ) -> _SentenceRuleT | None:
        return next(
            (
                rule
                for rule in rules
                if rule.when is None
                or sentence_meets_condition(
                    rule.when, sentence, location=_rule_location(rule)
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
        reduced_terms, term_reasons = _reduced_terms(rule=rule, sentence=sentence)
        return TermMultiplier(
            multiplier=TrackedValue(
                value=rule.multiplier,
                label=f"{sentence.sentence_id}.term_multiplier",
                rule_name=rule.name,
                formula="; ".join(
                    [
                        f"multiplier={rule.multiplier} because "
                        f"{_rule_match_reason(rule, sentence)}"
                    ]
                    + term_reasons
                ),
            ),
            reduced_terms=reduced_terms,
        )

    def get_credit_rate(self, sentence: Sentence) -> ProjectedCreditRate:
        """Returns the credit_rates credits for sentence, or no credits (a rate
        of 0.0 that reduces no terms) if the regime has none."""
        rule = self._first_matching_rule(self.regime.credit_rates, sentence)
        if rule is None:
            return ProjectedCreditRate(
                rate=TrackedValue(
                    value=0.0,
                    label=f"{sentence.sentence_id}.credit_rate",
                    formula="No credit_rates rule; default 0.0 days per month served",
                ),
                per_unit=1,
                unit=CreditRateUnit.MONTH,
                reduced_terms=frozenset(),
            )
        reduced_terms, term_reasons = _reduced_terms(rule=rule, sentence=sentence)
        return ProjectedCreditRate(
            rate=TrackedValue(
                value=rule.rate,
                label=f"{sentence.sentence_id}.credit_rate",
                rule_name=rule.name,
                formula="; ".join(
                    [
                        f"rate={rule.rate_description} because "
                        f"{_rule_match_reason(rule, sentence)}"
                    ]
                    + term_reasons
                ),
            ),
            per_unit=rule.per_unit,
            unit=rule.unit,
            reduced_terms=reduced_terms,
        )

    def get_credit_cap(
        self,
        *,
        sentence: Sentence,
        term: TermType,
        sentence_length_days: TrackedValue[int],
    ) -> CreditCap | None:
        """Returns the credit_caps cap on sentence's credits against term, or None
        if no cap applies to sentence or the cap that applies doesn't bind term.

        Args:
            sentence: The sentence earning credits.
            term: The term being capped.
            sentence_length_days: Length of term, in days. The rule's expression
                refers to it as sentence_length_days.
        """
        rule = self._first_matching_rule(self.regime.credit_caps, sentence)
        if rule is None or term not in rule.binds:
            return None
        return CreditCap(
            max_credit_days=evaluate_days_expression(
                expression=rule.max_credit_days,
                variables={SENTENCE_LENGTH_DAYS_VARIABLE: sentence_length_days},
                label=f"{sentence.sentence_id}.credit_cap_{term.value}",
                rule_name=rule.name,
            ),
            binds=rule.binds,
        )
