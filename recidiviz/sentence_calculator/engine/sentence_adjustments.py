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
"""Computes how a law regime shortens a single sentence's terms.

For example, under a regime whose term_multipliers give a 0.65 multiplier and flat
conversion (365 days per year), a sentence S1 with a 2 year min term, a 5 year max
term, and 30 days of jail credit is adjusted to:

    original_min_days: 730    adjusted_min_days: 444  (int(730 * 0.65) - 30)
    original_max_days: 1825   adjusted_max_days: 1156 (int(1825 * 0.65) - 30)

Under a regime whose credit_rates earn 20 days per 30-day month, the same sentence's
max term is instead reduced by the credits earned month by month while it is
served:

    original_max_days: 1825   adjusted_max_days: 1065

The 30 days of jail credit earn int(30 / 30 * 20) = 20 days of credit, leaving
1825 - 30 - 20 = 1775 days to cover. Each 30-day month served covers 50 of them
(30 served plus 20 credit), so 35 months cover 1750 and serve 1050 days. The last
25 days take 15 more days of serving, which earn a prorated 15 / 30 * 20 = 10 days
of credit, for 1050 + 15 = 1065 days served.
"""

import math
from datetime import date, timedelta
from fractions import Fraction

from dateutil.relativedelta import relativedelta

from recidiviz.sentence_calculator.engine.rule_engine import RuleEngine
from recidiviz.sentence_calculator.models.inputs import Sentence, Term
from recidiviz.sentence_calculator.models.outputs import (
    ProjectedCreditRate,
    TermMultiplier,
    TrackedSentenceAdjustment,
)
from recidiviz.sentence_calculator.models.rules import LawRegime, TermType
from recidiviz.sentence_calculator.models.tracked import TrackedValue

# Labels naming which of a sentence's two terms is being calculated. They are
# interpolated into TrackedValue labels, e.g. S1.adjusted_min_days.
# TODO(NV-48): Term types should be state-specific and these labels should probably
# be included in the state metadata.
MIN_TERM_LABEL = "min"
MAX_TERM_LABEL = "max"


def _term_parts(
    term: Term, label: str
) -> tuple[TrackedValue[int], TrackedValue[int], TrackedValue[int]]:
    """Returns term's years, months, and days as inputs labeled under label."""
    return (
        TrackedValue.input(term.years, f"{label}.years"),
        TrackedValue.input(term.months, f"{label}.months"),
        TrackedValue.input(term.days, f"{label}.days"),
    )


def _term_to_days(
    *, term: Term, label: str, start_date: TrackedValue[date], regime: LawRegime
) -> TrackedValue[int]:
    """Returns the number of days a term spans, converted the way the regime says.

    With calendar conversion, the term spans the real days from start_date, so a
    one year term starting 2024-01-01 is 366 days (because 2024 is a leap year with
    366 days). Otherwise it is
    years * regime.days_per_year + months * regime.days_per_month + days,
    with each product truncated to an int.

    Args:
        term: The term to convert.
        label: Label for the resulting TrackedValue, e.g. S1.original_max_days.
        start_date: The date the term starts counting from, used for calendar
            conversion. For a sentence's min and max terms this is its RETRO date.
        regime: The regime whose conversion settings apply.
    """
    years, months, days = _term_parts(term, label)

    if regime.use_calendar_term_conversion:
        end_date = start_date.value + relativedelta(
            years=term.years, months=term.months, days=term.days
        )
        total = (end_date - start_date.value).days
        return TrackedValue(
            value=total,
            label=label,
            sources=(years, months, days, start_date),
            formula=(
                f"{start_date.value} + {term.years}y {term.months}m {term.days}d = "
                f"{end_date} ({total} days)"
            ),
        )

    year_days = int(term.years * regime.days_per_year)
    month_days = int(term.months * regime.days_per_month)
    total = year_days + month_days + term.days
    return TrackedValue(
        value=total,
        label=label,
        sources=(years, months, days),
        formula=(
            f"{term.years}y × {regime.days_per_year} + "
            f"{term.months}m × {regime.days_per_month} + {term.days}d = {total} days"
        ),
    )


def _jail_credit_to_days(
    *, term: Term, label: str, start_date: TrackedValue[date], regime: LawRegime
) -> TrackedValue[int]:
    """Returns the number of days of jail credit term spans. Jail credit is time
    served before the sentence starts, so with calendar conversion it counts back
    from start_date: one month of jail credit on a sentence starting 2024-03-01 is
    the 29 days of February 2024. Otherwise it converts as _term_to_days does."""
    if not regime.use_calendar_term_conversion:
        return _term_to_days(
            term=term, label=label, start_date=start_date, regime=regime
        )
    years, months, days = _term_parts(term, label)
    credit_start_date = start_date.value - relativedelta(
        years=term.years, months=term.months, days=term.days
    )
    total = (start_date.value - credit_start_date).days
    return TrackedValue(
        value=total,
        label=label,
        sources=(years, months, days, start_date),
        formula=(
            f"{start_date.value} - {term.years}y {term.months}m {term.days}d = "
            f"{credit_start_date} ({total} days)"
        ),
    )


def _retro_date(
    *, start_date: TrackedValue[date], jail_credit: TrackedValue[int], label: str
) -> TrackedValue[date]:
    """Returns the RETRO date, start_date minus the days of jail credit: the day
    the person began serving time that counts toward the sentence."""
    retro_date = start_date.value - timedelta(days=jail_credit.value)
    return start_date.derive(
        new_value=retro_date,
        label=label,
        formula=f"{start_date.value} - {jail_credit.value} days = {retro_date}",
        other_sources=[jail_credit],
    )


def calculate_sentence_adjustment(
    *, sentence: Sentence, start_date: TrackedValue[date], engine: RuleEngine
) -> TrackedSentenceAdjustment:
    """Returns sentence's original and adjusted terms in days.

    Jail credit comes off both terms in use today. (This might change as we have
    more terms than MIN and MAX.) The regime's reduction, from its credit_rates or
    its term_multipliers, comes off each term that the matching rule's applies_to
    lists for the sentence.

    With calendar conversion, the min and max terms count from the RETRO date,
    the start date minus jail credit. A one year term starting 2024-03-15 with
    30 days of jail credit counts from 2024-02-14, so it spans February 29 and
    is 366 days. This might change as we generalize beyond NV.

    The adjusted days this function returns are not modified by a -1, so e.g.
    adding a sentence that 365 days after adjustments to a start date of 01/01/2023
    would give a result of 01/01/2024 (2023 is not a leap year).
    States that for whom a year sentence should end on the day prior (e.g. a one
    year sentence starting 01/01/2023 ends 12/31/2023) add this in the formulas in
    their YAML rules; this result is then consumed in those formulas.

    Args:
        sentence: The sentence to adjust.
        start_date: The date the sentence starts, used for calendar conversion,
            tracked back to the values it was computed from.
        engine: Rule engine for the regime that governs the sentence.
    """
    regime = engine.regime
    sentence_id = sentence.sentence_id
    jail_credit = _jail_credit_to_days(
        term=sentence.initial_time_served,
        label=f"{sentence_id}.jail_credit",
        start_date=start_date,
        regime=regime,
    )
    retro_date = _retro_date(
        start_date=start_date,
        jail_credit=jail_credit,
        label=f"{sentence_id}.retro_date",
    )
    original_min_days = _term_to_days(
        term=sentence.min_term,
        label=f"{sentence_id}.original_min_days",
        start_date=retro_date,
        regime=regime,
    )
    original_max_days = _term_to_days(
        term=sentence.max_term,
        label=f"{sentence_id}.original_max_days",
        start_date=retro_date,
        regime=regime,
    )

    if regime.credit_rates:
        credit_rate = engine.get_credit_rate(sentence)
        adjusted_min_days, _ = _apply_credit_rate(
            sentence=sentence,
            start_date=start_date.value,
            engine=engine,
            original_days=original_min_days,
            jail_credit=jail_credit,
            credit_rate=credit_rate,
            term=TermType.MIN_TERM,
            term_label=MIN_TERM_LABEL,
        )
        adjusted_max_days, max_term_reduction_days = _apply_credit_rate(
            sentence=sentence,
            start_date=start_date.value,
            engine=engine,
            original_days=original_max_days,
            jail_credit=jail_credit,
            credit_rate=credit_rate,
            term=TermType.MAX_TERM,
            term_label=MAX_TERM_LABEL,
        )
        return TrackedSentenceAdjustment(
            sentence_id=sentence_id,
            original_min_days=original_min_days,
            original_max_days=original_max_days,
            adjusted_min_days=adjusted_min_days,
            adjusted_max_days=adjusted_max_days,
            max_term_reduction_days=max_term_reduction_days,
            term_multiplier=None,
            credit_rate=credit_rate,
            jail_credit=jail_credit,
            retro_date=retro_date,
        )

    term_multiplier = engine.get_term_multiplier(sentence)
    adjusted_min_days, _ = _apply_term_multiplier(
        sentence_id=sentence_id,
        original_days=original_min_days,
        jail_credit=jail_credit,
        term_multiplier=term_multiplier,
        term=TermType.MIN_TERM,
        term_label=MIN_TERM_LABEL,
    )
    adjusted_max_days, max_term_reduction_days = _apply_term_multiplier(
        sentence_id=sentence_id,
        original_days=original_max_days,
        jail_credit=jail_credit,
        term_multiplier=term_multiplier,
        term=TermType.MAX_TERM,
        term_label=MAX_TERM_LABEL,
    )
    return TrackedSentenceAdjustment(
        sentence_id=sentence_id,
        original_min_days=original_min_days,
        original_max_days=original_max_days,
        adjusted_min_days=adjusted_min_days,
        adjusted_max_days=adjusted_max_days,
        max_term_reduction_days=max_term_reduction_days,
        term_multiplier=term_multiplier,
        credit_rate=None,
        jail_credit=jail_credit,
        retro_date=retro_date,
    )


def _subtract_jail_credit_only(
    *,
    sentence_id: str,
    original_days: TrackedValue[int],
    jail_credit: TrackedValue[int],
    unapplied_reduction: TrackedValue[float],
    term_label: str,
) -> tuple[TrackedValue[int], TrackedValue[int]]:
    """Returns a tuple where the first item is the term after subtracting only jail
    credit, and the second is 0 days -- which is the amount that jail credit is
    reduced by whatever reduction the regime uses. This form satisfies the expectation
    of callers, who get the form [reduced term, days reduced] from other sources.
    unapplied_reduction is the regime's multiplier/credit rate, which says why the
    isn't used on the jail credits but is cited in the explanation of the credits not
    being reduced."""
    adjusted = max(0, original_days.value - jail_credit.value)
    return (
        original_days.derive(
            new_value=adjusted,
            label=f"{sentence_id}.adjusted_{term_label}_days",
            formula=(
                f"max(0, {original_days.value} - {jail_credit.value}) = "
                f"{adjusted} ({unapplied_reduction.label} not applied)"
            ),
            other_sources=[unapplied_reduction, jail_credit],
        ),
        TrackedValue(
            value=0,
            label=f"{sentence_id}.{term_label}_term_reduction_days",
            sources=(unapplied_reduction,),
            formula=f"0 ({unapplied_reduction.label} not applied)",
        ),
    )


def _apply_term_multiplier(
    *,
    sentence_id: str,
    original_days: TrackedValue[int],
    jail_credit: TrackedValue[int],
    term_multiplier: TermMultiplier,
    term: TermType,
    term_label: str,
) -> tuple[TrackedValue[int], TrackedValue[int]]:
    """Returns the term after subtracting jail credit and multiplying by the
    multiplier, if appropriate, and the days the multiplier took off the term.
    Those days count in full even when jail credit covers the rest of the term,
    and are 0 when the multiplier doesn't reduce the term. The derivation trees
    keep the multiplier in the formula, so they say why the reduction does or
    doesn't reach the term."""
    multiplier = term_multiplier.multiplier
    if term not in term_multiplier.reduced_terms:
        return _subtract_jail_credit_only(
            sentence_id=sentence_id,
            original_days=original_days,
            jail_credit=jail_credit,
            unapplied_reduction=multiplier,
            term_label=term_label,
        )
    # Fraction keeps the product exact; with floats, 90 days × 0.7 truncates to 62.
    # TODO(NV-9): OSM rounds credits up to a whole day. Truncating the reduced term
    # rounds the reduction up too, but confirm that's how NDOC wants to apply a term
    # multiplier.
    reduced_days = int(Fraction(original_days.value) * _to_fraction(multiplier.value))
    adjusted = max(0, reduced_days - jail_credit.value)
    reduction_days = original_days.value - reduced_days
    return (
        original_days.derive(
            new_value=adjusted,
            label=f"{sentence_id}.adjusted_{term_label}_days",
            formula=(
                f"max(0, int({original_days.value} × {multiplier.value}) - "
                f"{jail_credit.value}) = {adjusted}"
            ),
            other_sources=[multiplier, jail_credit],
        ),
        original_days.derive(
            new_value=reduction_days,
            label=f"{sentence_id}.{term_label}_term_reduction_days",
            formula=(
                f"{original_days.value} - int({original_days.value} × "
                f"{multiplier.value}) = {reduction_days}"
            ),
            other_sources=[multiplier],
        ),
    )


def _apply_credit_rate(
    *,
    sentence: Sentence,
    start_date: date,
    engine: RuleEngine,
    original_days: TrackedValue[int],
    jail_credit: TrackedValue[int],
    credit_rate: ProjectedCreditRate,
    term: TermType,
    term_label: str,
) -> tuple[TrackedValue[int], TrackedValue[int]]:
    """Returns the days served from start_date until the term is complete, after
    jail credit and the credits projected by credit_rate are applied. When 'term'
    isn't one of credit_rate's reduced_terms, it returns the term minus only jail
    credit, and 0 credits.

    Credits accrue on the jail credit time at the regime's days_per_month, then
    month by month while the term is served: each full month earns credit_rate
    days, and the final, partial month earns credit prorated by the share of it
    served. The total is capped if the regime's CREDIT_CAP rule binds this term.

    TODO(NV-44): Use the credits a person has actually earned for time already
    served, and project credits only for the time not yet served. Today every
    credit is projected from start_date.
    """
    tracked_rate = credit_rate.rate
    if term not in credit_rate.reduced_terms:
        return _subtract_jail_credit_only(
            sentence_id=sentence.sentence_id,
            original_days=original_days,
            jail_credit=jail_credit,
            unapplied_reduction=tracked_rate,
            term_label=term_label,
        )
    regime = engine.regime
    rate = _to_fraction(tracked_rate.value)
    # TODO(NV-61): Use the credits NDOC recorded for jail credit time. This is the
    # only use of days_per_month under calendar term conversion, so it goes away
    # with this projection.
    jail_credit_credit_days = int(
        Fraction(jail_credit.value) / _to_fraction(regime.days_per_month) * rate
    )
    days_to_cover = original_days.value - jail_credit.value - jail_credit_credit_days
    served_days = _days_served_while_earning_credits(
        days_to_cover=days_to_cover,
        start_date=start_date,
        rate=rate,
        days_per_month=(
            None
            if regime.use_calendar_term_conversion
            else _to_fraction(regime.days_per_month)
        ),
    )
    # Jail credit can be longer than the term, when someone arrives with more
    # jail credit than days to serve and is released right away. The term then
    # needs no credits, rather than negative ones.
    uncapped_credit_days = max(0, original_days.value - jail_credit.value - served_days)
    uncapped_credits_formula = (
        f"{tracked_rate.value} days/month on {jail_credit.value} days of jail credit "
        f"and month by month from {start_date} = {uncapped_credit_days}"
    )
    credits_label = f"{sentence.sentence_id}.good_time_credits_{term_label}"

    credit_cap = engine.get_credit_cap(
        sentence=sentence, term=term, sentence_length_days=original_days
    )
    if credit_cap is None:
        good_time_credits = TrackedValue(
            value=uncapped_credit_days,
            label=credits_label,
            sources=(original_days, tracked_rate, jail_credit),
            formula=uncapped_credits_formula,
        )
    else:
        uncapped_credits = TrackedValue(
            value=uncapped_credit_days,
            label=f"{sentence.sentence_id}.uncapped_good_time_credits_{term_label}",
            sources=(original_days, tracked_rate, jail_credit),
            formula=uncapped_credits_formula,
        )
        max_credit_days = credit_cap.max_credit_days
        capped_credit_days = min(uncapped_credit_days, max_credit_days.value)
        good_time_credits = uncapped_credits.derive(
            new_value=capped_credit_days,
            label=credits_label,
            formula=(
                f"min({uncapped_credit_days}, {max_credit_days.value}) = "
                f"{capped_credit_days}"
            ),
            other_sources=[max_credit_days],
        )
    credit_days = good_time_credits.value
    adjusted = max(0, original_days.value - jail_credit.value - credit_days)
    return (
        original_days.derive(
            new_value=adjusted,
            label=f"{sentence.sentence_id}.adjusted_{term_label}_days",
            formula=(
                f"max(0, {original_days.value} - {jail_credit.value} - "
                f"{credit_days}) = {adjusted}"
            ),
            other_sources=[jail_credit, good_time_credits],
        ),
        good_time_credits,
    )


def _days_served_while_earning_credits(
    *,
    days_to_cover: int,
    start_date: date,
    rate: Fraction,
    days_per_month: Fraction | None,
) -> int:
    """Returns how many days starting at start_date must be served to cover
    days_to_cover, when each day served also earns credit.

    Each full month served covers its own days plus rate days of credit. In the
    final month, serving d days earns rate * d / (days in that month) of credit,
    so it ends on the first whole day where d plus that credit covers what is
    left.

    Args:
        days_to_cover: Days of the term left after jail credit and its credits.
        start_date: The first day served.
        rate: Credit days earned per full month served.
        days_per_month: Days in every month, or None to step through calendar
            months from start_date.
    """
    if days_to_cover <= 0:
        return 0
    remaining = Fraction(days_to_cover)
    served_days = Fraction(0)
    # TODO(NV-63): OSM counts credits by calendar month, from the 1st to the end of
    # each month, and prorates the first and last months by the days served in
    # them, rounding each prorated credit up. This loop steps months from
    # start_date instead, so a projected date can be a day off from OSM's.
    # Stepping from each month_end also clamps the day of the month for good: a
    # sentence starting January 31 steps to February 28, then to March 28 and
    # the 28th of every month after.
    month_start = start_date
    while True:
        month_end = month_start + relativedelta(months=1)
        days_in_month = (
            days_per_month
            if days_per_month is not None
            else Fraction((month_end - month_start).days)
        )
        if remaining <= days_in_month + rate:
            return math.ceil(served_days + remaining / (1 + rate / days_in_month))
        remaining -= days_in_month + rate
        served_days += days_in_month
        month_start = month_end


def _to_fraction(value: float) -> Fraction:
    """Returns value as an exact fraction of the decimal it prints as, so a
    multiplier of 0.65, a rate of 20.0, or a month of 30.44 days is used
    exactly rather than as its binary approximation."""
    return Fraction(repr(value))
