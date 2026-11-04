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
"""

from datetime import date, timedelta
from fractions import Fraction

from dateutil.relativedelta import relativedelta

from recidiviz.sentence_calculator.engine.rule_engine import RuleEngine
from recidiviz.sentence_calculator.models.inputs import Sentence, Term
from recidiviz.sentence_calculator.models.outputs import (
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
    *, sentence: Sentence, start_date: date, engine: RuleEngine
) -> TrackedSentenceAdjustment:
    """Returns sentence's original and adjusted terms in days.

    Jail credit comes off both terms in use today. (This might change as we have
    more terms than MIN and MAX.) The regime's reduction, from its term_multipliers,
    comes off each term it `applies_to` in the the reduction conditionals.

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
        start_date: The date the sentence starts, used for calendar conversion.
        engine: Rule engine for the regime that governs the sentence.
    """
    regime = engine.regime
    sentence_id = sentence.sentence_id
    tracked_start_date = TrackedValue.input(start_date, f"{sentence_id}.start_date")
    jail_credit = _jail_credit_to_days(
        term=sentence.initial_time_served,
        label=f"{sentence_id}.jail_credit",
        start_date=tracked_start_date,
        regime=regime,
    )
    retro_date = _retro_date(
        start_date=tracked_start_date,
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

    term_multiplier = engine.get_term_multiplier(sentence)
    return TrackedSentenceAdjustment(
        sentence_id=sentence_id,
        original_min_days=original_min_days,
        original_max_days=original_max_days,
        adjusted_min_days=_apply_term_multiplier(
            sentence_id=sentence_id,
            original_days=original_min_days,
            jail_credit=jail_credit,
            term_multiplier=term_multiplier,
            term=TermType.MIN_TERM,
            term_label=MIN_TERM_LABEL,
        ),
        adjusted_max_days=_apply_term_multiplier(
            sentence_id=sentence_id,
            original_days=original_max_days,
            jail_credit=jail_credit,
            term_multiplier=term_multiplier,
            term=TermType.MAX_TERM,
            term_label=MAX_TERM_LABEL,
        ),
        term_multiplier=term_multiplier,
        jail_credit=jail_credit,
    )


def _apply_term_multiplier(
    *,
    sentence_id: str,
    original_days: TrackedValue[int],
    jail_credit: TrackedValue[int],
    term_multiplier: TermMultiplier,
    term: TermType,
    term_label: str,
) -> TrackedValue[int]:
    """Returns term after subtracting jail credit and multiplying by the multiplier,
    if appropriate. The derivation tree keeps the multiplier in the formula, so it says
    why the reduction does or doesn't reach the term."""
    multiplier = term_multiplier.multiplier
    label = f"{sentence_id}.adjusted_{term_label}_days"
    if term not in term_multiplier.reduced_terms:
        adjusted = max(0, original_days.value - jail_credit.value)
        return original_days.derive(
            new_value=adjusted,
            label=label,
            formula=(
                f"max(0, {original_days.value} - {jail_credit.value}) = "
                f"{adjusted} ({multiplier.label} not applied)"
            ),
            other_sources=[multiplier, jail_credit],
        )
    # Fraction keeps the product exact; with floats, 90 days × 0.7 truncates to 62.
    # TODO(NV-9): OSM rounds credits up to a whole day. Truncating the reduced term
    # rounds the reduction up too, but confirm that's how NDOC wants to apply a term
    # multiplier.
    reduced_days = int(Fraction(original_days.value) * _to_fraction(multiplier.value))
    adjusted = max(0, reduced_days - jail_credit.value)
    return original_days.derive(
        new_value=adjusted,
        label=label,
        formula=(
            f"max(0, int({original_days.value} × {multiplier.value}) - "
            f"{jail_credit.value}) = {adjusted}"
        ),
        other_sources=[multiplier, jail_credit],
    )


def _to_fraction(value: float) -> Fraction:
    """Returns value as an exact fraction of the decimal it prints as, so a
    multiplier of 0.65 is used as 13/20 rather than its binary approximation."""
    return Fraction(repr(value))
