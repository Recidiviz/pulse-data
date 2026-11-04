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
"""Output data models for sentence calculator."""

from datetime import date, timedelta

import attr

from recidiviz.common import attr_validators
from recidiviz.sentence_calculator.models.rules import TermType
from recidiviz.sentence_calculator.models.tracked import TrackedValue


@attr.define(frozen=True, kw_only=True)
class GraphEdge:
    """An edge in the sentence DAG."""

    source: str = attr.ib(validator=attr_validators.is_non_empty_str)
    target: str = attr.ib(validator=attr_validators.is_non_empty_str)


@attr.define(frozen=True, kw_only=True)
class DisplayGraph:
    """DAG structure showing sentence relationships.

    This is used for visualizing the sentence graph in the UI."""

    nodes: list[str] = attr.ib(validator=attr_validators.is_list_of(str))
    edges: list[GraphEdge] = attr.ib(validator=attr_validators.is_list_of(GraphEdge))


# ==============================================================================
# TRACKED OUTPUT MODELS (with derivation trees)
# ==============================================================================


@attr.define(frozen=True, kw_only=True)
class TermMultiplier:
    """The flat reduction a regime's term_multipliers apply to a sentence's terms."""

    multiplier: TrackedValue[float] = attr.ib(
        validator=attr.validators.instance_of(TrackedValue)
    )
    """Fraction of each reduced term left after the reduction, e.g. 0.65."""

    reduced_terms: frozenset[TermType] = attr.ib(
        validator=attr_validators.is_frozenset_of(TermType)
    )
    """The terms the reduction comes off. Empty when the regime has no
    term_multipliers rule for the sentence, or when the matching rule's
    applies_to reaches none of the sentence's terms."""


@attr.define(frozen=True, kw_only=True)
class TrackedSentenceAdjustment:
    """Sentence adjustment with full derivation trees."""

    sentence_id: str = attr.ib(validator=attr_validators.is_non_empty_str)
    # MIN date (time to parole eligibility) in days, as ordered by the court
    original_min_days: "TrackedValue[int]" = attr.ib(
        validator=attr.validators.instance_of(TrackedValue)
    )
    # MAX date (time to release) in days, as ordered by the court
    original_max_days: "TrackedValue[int]" = attr.ib(
        validator=attr.validators.instance_of(TrackedValue)
    )
    # MIN date (time to parole eligibility) in days, adjusted for earned or projected
    # credit, jail time, or other adjustments.
    adjusted_min_days: "TrackedValue[int]" = attr.ib(
        validator=attr.validators.instance_of(TrackedValue)
    )
    # MAX date (time to release) in days, adjusted for earned or projected credit, jail
    # time, or other adjustments.
    adjusted_max_days: "TrackedValue[int]" = attr.ib(
        validator=attr.validators.instance_of(TrackedValue)
    )
    # The regime's flat reduction (e.g. a .65 multiplier for the 35% off in SB413) and
    # the terms it comes off. Only jail credit comes off a term it doesn't reduce.
    term_multiplier: "TermMultiplier" = attr.ib(
        validator=attr.validators.instance_of(TermMultiplier)
    )
    # Jail credit to apply to root sentences (those consecutive to no other sentences)
    jail_credit: "TrackedValue[int]" = attr.ib(
        validator=attr.validators.instance_of(TrackedValue)
    )


@attr.define(frozen=True, kw_only=True)
class SentenceTermCalculation:
    """The result of computing a single sentence's term: when it starts, how the
    regime adjusts it, its length in days, the date it ends, and the date it
    reaches its (not yet overlap-adjusted) parole eligibility."""

    start_date: TrackedValue[date] = attr.ib(
        validator=attr.validators.instance_of(TrackedValue)
    )
    """Date the sentence starts: its own start date for a root sentence, or the
    day after its parent ends for a consecutive sentence."""

    adjustment: TrackedSentenceAdjustment = attr.ib(
        validator=attr.validators.instance_of(TrackedSentenceAdjustment)
    )
    """The sentence's original and regime-adjusted min and max terms."""

    sentence_ped: TrackedValue[date] = attr.ib(
        validator=attr.validators.instance_of(TrackedValue)
    )
    """Date the sentence becomes eligible to go before the parole board: its start
    date plus its adjusted min term."""

    @property
    def total_days(self) -> TrackedValue[int]:
        """The sentence's regime-adjusted max term, in days."""
        return self.adjustment.adjusted_max_days

    @property
    def end_date(self) -> date:
        """Last day served on the sentence's term (inclusive). A consecutive
        sentence starts the day after. The start date is the first day served, so
        a term of n days ends n - 1 days after the start."""
        # TODO(NV-49): An adjusted term can be 0 days, when jail credit covers the
        # whole term or a term multiplier truncates a 1 day term to 0. The - 1 then
        # puts end_date the day before start_date, so a Nevada sentence can end the day
        # before it begins. Revisit once we better understand how this case should
        # be handled for different start dates.
        return self.start_date.value + timedelta(days=self.total_days.value - 1)


@attr.define(frozen=True, kw_only=True)
class TrackedLongestSentencePath:
    """Aggregated result with derivation."""

    # Sentence IDs representing the path of 'controlling' sentences
    path_sentence_ids: list[str] = attr.ib(validator=attr_validators.is_list_of(str))
    # Total days to be served for this sentence. Tracked value shows how many days each
    # sentence adds to the total.
    # TODO(NV-9): Validate that we actually need this value to be tracked.
    total_days: "TrackedValue[int]" = attr.ib(
        validator=attr.validators.instance_of(TrackedValue)
    )
    # Latest date any sentence becomes eligible to go before the parole board. Tracked
    # value shows derivation from the sentence's MIN and start date (derived from
    # earlier sentences in the chain if it's not a root sentence).
    max_ped: "TrackedValue[date]" = attr.ib(
        validator=attr.validators.instance_of(TrackedValue)
    )
    # The term calculation for every sentence in the graph, keyed by sentence ID.
    sentence_terms: dict[str, SentenceTermCalculation] = attr.ib(
        validator=attr_validators.is_dict_of(str, SentenceTermCalculation)
    )
