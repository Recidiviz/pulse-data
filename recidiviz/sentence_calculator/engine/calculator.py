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
"""Calculates the controlling sentences and projected dates for a person's
sentences under a law regime.
"""

from collections.abc import Sequence
from datetime import date, timedelta

from dateutil.relativedelta import relativedelta

from recidiviz.sentence_calculator.engine.date_formulas import (
    DateCalculationContext,
    evaluate_date_formulas,
)
from recidiviz.sentence_calculator.engine.rule_engine import RuleEngine
from recidiviz.sentence_calculator.engine.sentence_graph import SentenceGraph
from recidiviz.sentence_calculator.models.inputs import (
    Sentence,
    SentenceCalculationInput,
    SentenceType,
)
from recidiviz.sentence_calculator.models.outputs import (
    SentenceCalculationResult,
    TrackedLongestSentencePath,
    TrackedProjectedDates,
)
from recidiviz.sentence_calculator.models.rules import LawRegime
from recidiviz.sentence_calculator.models.tracked import TrackedValue, tracked_sum
from recidiviz.sentence_calculator.rules.loader import load_law_regime

# Ordered most to least severe. A calculation reports the most severe
# indeterminate type among its sentences.
_SPECIAL_SENTENCE_TYPES_BY_SEVERITY = [
    SentenceType.DEATH,
    SentenceType.LWO,
    SentenceType.LIFE,
]


def calculate_sentences(
    calculation_input: SentenceCalculationInput,
) -> SentenceCalculationResult:
    """Returns the controlling sentences and projected dates for the input's
    sentences under the input's law regime."""
    # TODO(NV-40): Cache loaded regimes. This re-reads and re-parses the regime's
    # YAML on every calculation, which dominates the cost for most people.
    # TODO(NV-53): We might load the regime after the DEATH/LWO return below, which
    # doesn't use it. This waits until those sentence types are fully supported,
    # since we don't yet know what the UI will need returned for them.
    engine = RuleEngine(
        load_law_regime(
            state_code=calculation_input.state_code,
            regime_name=calculation_input.law_regime_id,
        )
    )
    engine.check_sentence_fields(calculation_input.sentences)
    graph = SentenceGraph(calculation_input.sentences)
    indeterminate_sentence_type = _most_severe_indeterminate_type(
        calculation_input.sentences
    )
    if indeterminate_sentence_type in (SentenceType.DEATH, SentenceType.LWO):
        return SentenceCalculationResult(
            input=calculation_input,
            indeterminate_sentence_type=indeterminate_sentence_type,
            graph=graph.display_graph,
            longest_path=None,
            projected_dates=None,
        )

    longest_path = graph.longest_path(engine=engine)
    projected_dates = (
        _calculate_projected_dates(
            engine=engine,
            sentences=calculation_input.sentences,
            longest_path=longest_path,
        )
        if longest_path is not None and indeterminate_sentence_type is None
        else None
    )
    return SentenceCalculationResult(
        input=calculation_input,
        indeterminate_sentence_type=indeterminate_sentence_type,
        graph=graph.display_graph,
        longest_path=longest_path,
        projected_dates=projected_dates,
    )


def _most_severe_indeterminate_type(
    sentences: Sequence[Sentence],
) -> SentenceType | None:
    """Returns the most severe of DEATH, LWO, and LIFE among sentences' types, or
    None if no sentence has one of those types."""
    sentence_types = {sentence.sentence_type for sentence in sentences}
    return next(
        (t for t in _SPECIAL_SENTENCE_TYPES_BY_SEVERITY if t in sentence_types), None
    )


def _calculate_projected_dates(
    *,
    engine: RuleEngine,
    sentences: Sequence[Sentence],
    longest_path: TrackedLongestSentencePath,
) -> TrackedProjectedDates:
    """Returns the regime's projected dates, evaluated against values summed over
    the controlling path's sentences and the latest PED across all sentences.
    Only called when no sentence is DEATH, LWO, or LIFE."""
    sentences_by_id = {s.sentence_id: s for s in sentences}
    path_terms = [
        longest_path.sentence_terms[sentence_id]
        for sentence_id in longest_path.path_sentence_ids
    ]
    path_adjustments = [term.adjustment for term in path_terms]

    # TODO(NV-9): Summing each controlling sentence's full term can lead to
    # overcounting if they overlap.
    earliest_start_date = path_terms[0].start_date
    unadjusted_max_days = tracked_sum(
        [a.original_max_days for a in path_adjustments], "unadjusted_max_days"
    )
    # TODO(NV-9): This reuses the graph's running total, so derivation trees show
    # the graph's labels rather than adjusted_max_days. Once the overlap fix
    # settles how total_days is computed, decide whether to sum the path's
    # adjusted max terms with tracked_sum like the other values here.
    adjusted_max_days = longest_path.total_days
    jail_credit_days = tracked_sum(
        [a.jail_credit for a in path_adjustments], "jail_credit_days"
    )
    good_time_days = tracked_sum(
        [a.max_term_reduction_days for a in path_adjustments], "good_time_days"
    )
    # The terms count from the RETRO date, so their whole years do too.
    # TODO(NV-9): This uses only the first path sentence's jail credit, which is
    # right for one consecutive chain. A path that picks up a later chain whose
    # root has its own jail credit needs that chain's RETRO date too.
    first_jail_credit = path_adjustments[0].jail_credit
    retro_date = earliest_start_date.derive(
        new_value=earliest_start_date.value - timedelta(days=first_jail_credit.value),
        label="retro_date",
        formula=f"{earliest_start_date.label} - {first_jail_credit.label}",
        other_sources=[first_jail_credit],
    )
    context = DateCalculationContext(
        earliest_start_date=earliest_start_date,
        unadjusted_min_days=tracked_sum(
            [a.original_min_days for a in path_adjustments], "unadjusted_min_days"
        ),
        unadjusted_max_days=unadjusted_max_days,
        unadjusted_max_years=_whole_years(
            days=unadjusted_max_days,
            retro_date=retro_date,
            regime=engine.regime,
            label="unadjusted_max_years",
        ),
        adjusted_min_days=tracked_sum(
            [a.adjusted_min_days for a in path_adjustments], "adjusted_min_days"
        ),
        adjusted_max_days=adjusted_max_days,
        jail_credit_days=jail_credit_days,
        good_time_days=good_time_days,
        max_ped=longest_path.max_ped,
        sentences=[
            sentences_by_id[sentence_id]
            for sentence_id in longest_path.path_sentence_ids
        ],
    )
    return TrackedProjectedDates(
        earliest_start_date=earliest_start_date,
        dates=evaluate_date_formulas(regime=engine.regime, context=context),
    )


def _whole_years(
    *,
    days: TrackedValue[int],
    retro_date: TrackedValue[date],
    regime: LawRegime,
    label: str,
) -> TrackedValue[int]:
    """Returns the whole years in days, counted however the regime converts terms to
    days. With calendar term conversion, that's calendar years from retro_date,
    the date the terms start counting from, so 1095 days from 2024-01-01 is 2
    years because 2024 is a leap year. Otherwise every year is
    regime.days_per_year days."""
    # TODO(NV-47): Even if this is just for display, it seems like it shouldn't be an
    # int? Surely we could / should round to one decimal place?
    # Alternatively we may not need whole years at all, and can rip this out.
    if regime.use_calendar_term_conversion:
        end_date = retro_date.value + timedelta(days=days.value)
        return TrackedValue(
            value=relativedelta(end_date, retro_date.value).years,
            label=label,
            sources=(days, retro_date),
            formula=(
                f"whole calendar years from {retro_date.label} to "
                f"{retro_date.label} + {days.label}"
            ),
        )
    return TrackedValue(
        value=int(days.value // regime.days_per_year),
        label=label,
        sources=(days,),
        formula=f"{days.label} // {regime.days_per_year}",
    )
