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
"""Tests for SentenceGraph."""

import unittest
from datetime import date

from recidiviz.sentence_calculator.engine.rule_engine import RuleEngine
from recidiviz.sentence_calculator.engine.sentence_graph import SentenceGraph
from recidiviz.sentence_calculator.models.inputs import Sentence, SentenceType, Term
from recidiviz.sentence_calculator.models.rules import (
    LawRegime,
    ReductionTarget,
    TermMultiplierRule,
    TermType,
)
from recidiviz.sentence_calculator.models.state_inputs.inputs_nv import (
    NevadaOffenseCategory,
    NevadaOffenseGroup,
    NevadaSentence,
)

# A regime with no reductions and flat 365/30 conversion, so each sentence's adjusted
# terms are its original terms less any jail credit. Tests of the traversal itself use it so
# term lengths can be read directly off the inputs.
_NO_ADJUSTMENT_ENGINE = RuleEngine(
    LawRegime(
        name="NO_ADJUSTMENT",
        description="Regime that leaves every term unchanged",
        effective_date=date(2020, 1, 1),
        use_calendar_term_conversion=False,
        days_per_month=30.0,
        days_per_year=365.0,
        term_multipliers=[],
        credit_rates=[],
        credit_caps=[],
        projected_dates={},
    )
)
_CALENDAR_NO_ADJUSTMENT_ENGINE = RuleEngine(
    LawRegime(
        name="CALENDAR_NO_ADJUSTMENT",
        description="Regime that counts terms by calendar and leaves them unchanged",
        effective_date=date(2020, 1, 1),
        use_calendar_term_conversion=True,
        days_per_month=30.0,
        days_per_year=365.0,
        term_multipliers=[],
        credit_rates=[],
        credit_caps=[],
        projected_dates={},
    )
)


def _sentence(
    sentence_id: str,
    *,
    parent_sentence_id: str | None = None,
    start_date: date | None = None,
    min_term: Term = Term(),
    max_term: Term = Term(),
    initial_time_served: Term = Term(),
    offense_category: NevadaOffenseCategory = NevadaOffenseCategory.CATEGORY_C,
    offense_group: NevadaOffenseGroup = NevadaOffenseGroup.OTHER,
) -> Sentence:
    """Returns a Nevada sentence with the given fields.

    SentenceGraph compares active sentences with NvSentenceWithSortableEndDate,
    whose tie-breaks require a Nevada offense category and group. Most tests
    don't care which ones, so they default to a mid-severity category and a
    non-severe group.
    """
    # Pylint known false positive
    return NevadaSentence(  # pylint: disable=abstract-class-instantiated
        sentence_id=sentence_id,
        offense_id=9999,
        min_term=min_term,
        max_term=max_term,
        initial_time_served=initial_time_served,
        offense_category=offense_category,
        offense_group=offense_group,
        sentence_type=SentenceType.OTHER,
        parent_sentence_id=parent_sentence_id,
        start_date=start_date,
    )


class TestSentenceGraphConstruction(unittest.TestCase):
    """Tests for SentenceGraph's graph-building and display_graph."""

    def test_display_graph_matches_graph_structure(self) -> None:
        graph = SentenceGraph(
            [_sentence("S1"), _sentence("S2", parent_sentence_id="S1")]
        )

        display_graph = graph.display_graph

        self.assertEqual(set(display_graph.nodes), {"S1", "S2"})
        self.assertEqual(len(display_graph.edges), 1)
        self.assertEqual(display_graph.edges[0].source, "S1")
        self.assertEqual(display_graph.edges[0].target, "S2")

    def test_cycle_surfaces_error(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"^Input graph contains cycles, check for errors\.$"
        ):
            SentenceGraph(
                [
                    _sentence("S1", parent_sentence_id="S2"),
                    _sentence("S2", parent_sentence_id="S1"),
                ]
            )

    def test_consecutive_sentence_with_start_date_surfaces_error(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Consecutive sentence \[S2\] has start date \[2024-04-11\]\. The parent "
            r"sentence is \[S1\]\. Remove the start date from the consecutive "
            r"sentence\.$",
        ):
            SentenceGraph(
                [
                    _sentence(
                        "S1", start_date=date(2024, 1, 1), max_term=Term(days=100)
                    ),
                    _sentence(
                        "S2",
                        parent_sentence_id="S1",
                        start_date=date(2024, 4, 11),
                        max_term=Term(days=100),
                    ),
                ]
            )

    def test_consecutive_sentence_with_jail_credit_surfaces_error(self) -> None:
        root = _sentence("S1", start_date=date(2024, 1, 1), max_term=Term(days=100))
        consecutive = _sentence(
            "S2",
            parent_sentence_id="S1",
            max_term=Term(days=100),
            initial_time_served=Term(days=30),
        )

        with self.assertRaisesRegex(
            ValueError,
            r"^Consecutive sentence \[S2\] has jail credit "
            r"\[Term\(years=0, months=0, days=30\)\]\. Jail credit applies to the "
            r"root sentence of a chain, which here is reached through \[S1\]\. Move "
            r"the jail credit to the root sentence\.$",
        ):
            SentenceGraph([root, consecutive])


class TestSentenceGraphLongestPath(unittest.TestCase):
    """Tests for SentenceGraph.longest_path()."""

    def test_empty_graph_returns_none(self) -> None:
        graph = SentenceGraph([])

        result = graph.longest_path(engine=_NO_ADJUSTMENT_ENGINE)

        self.assertIsNone(result)

    def test_single_sentence_returns_its_own_term_length(self) -> None:
        graph = SentenceGraph(
            [_sentence("S1", start_date=date(2024, 1, 1), max_term=Term(days=100))]
        )

        result = graph.longest_path(engine=_NO_ADJUSTMENT_ENGINE)

        assert result is not None
        self.assertEqual(result.path_sentence_ids, ["S1"])
        self.assertEqual(result.total_days.value, 100)

    def test_path_goes_from_earliest_to_latest_not_just_longest(
        self,
    ) -> None:
        """S1, S2, and S3 are concurrently sentences.

        S1 starts first and is 1 year long. S2 starts in the middle of S1 and
        is the longest of the three at 7 years. S3 starts before S2 ends and
        ends after S2.

        This handles the case where the longest chain (here just S2) is not the
        chain with the earliest start or the latest end.
        """
        graph = SentenceGraph(
            [
                _sentence("S1", start_date=date(2024, 1, 1), max_term=Term(years=1)),
                _sentence("S2", start_date=date(2024, 7, 1), max_term=Term(years=7)),
                _sentence("S3", start_date=date(2030, 7, 1), max_term=Term(years=3)),
            ]
        )

        result = graph.longest_path(engine=_NO_ADJUSTMENT_ENGINE)

        assert result is not None
        self.assertEqual(result.path_sentence_ids, ["S1", "S2", "S3"])

    def test_root_sentence_without_start_date_surfaces_error(self) -> None:
        """S1 and S2 run concurrently. S2 has a start date. S1 does not.

        longest_path needs a start date for every root sentence. A root
        sentence without one is not silently skipped.
        """
        graph = SentenceGraph(
            [
                _sentence("S1", max_term=Term(days=100)),
                _sentence("S2", start_date=date(2024, 1, 1), max_term=Term(days=100)),
            ]
        )

        with self.assertRaisesRegex(
            ValueError, r"^No start date for root / Level 1 sentence \[S1\]$"
        ):
            graph.longest_path(engine=_NO_ADJUSTMENT_ENGINE)

    def test_concurrent_chains_pick_the_active_sentence_not_one_per_index(
        self,
    ) -> None:
        """Two sentence chains run at the same time. Each chain is a set of
        consecutive sentences.

        Chain 1 has four sentences, in this order:
        a) 5 years
        b) 1 year
        c) 1 year
        d) 5 years

        Chain 2 has two sentences, in this order:
        a) 3 years
        b) 5 years
        Both chains start on the same date.

        The resulting path is C1a, C2b, C1d. It skips C1b and C1c in chain 1.
        It also skips C2a in chain 2.

        The algorithm does not pick one sentence per position across the two
        chains. It tracks which sentence is active on a given date. At each
        step, it picks whichever active sentence ends last.
        """
        c1a = _sentence("C1a", start_date=date(2024, 1, 1), max_term=Term(years=5))
        c1b = _sentence(
            "C1b",
            parent_sentence_id="C1a",
            max_term=Term(years=1),
        )
        c1c = _sentence(
            "C1c",
            parent_sentence_id="C1b",
            max_term=Term(years=1),
        )
        c1d = _sentence(
            "C1d",
            parent_sentence_id="C1c",
            max_term=Term(years=5),
        )

        c2a = _sentence("C2a", start_date=date(2024, 1, 1), max_term=Term(years=3))
        c2b = _sentence(
            "C2b",
            parent_sentence_id="C2a",
            max_term=Term(years=5),
        )

        graph = SentenceGraph([c1a, c1b, c1c, c1d, c2a, c2b])

        result = graph.longest_path(engine=_NO_ADJUSTMENT_ENGINE)

        assert result is not None
        self.assertEqual(result.path_sentence_ids, ["C1a", "C2b", "C1d"])
        self.assertEqual(result.total_days.value, 5475)

    def test_shorter_chain_sentence_becomes_controlling_before_its_chain_ends(
        self,
    ) -> None:
        """Two sentence chains run concurrently, each with two sentences.

        Chain 1: C1a (2 years), then C1b (3 years).
        Chain 2: C2a (1 year), then C2b (2 years).
        Both chains start on the same date.

        C2a is shorter than C1a, so C2b starts while C1a is still running. C2b's end
        date is later than C1a's, so C2b becomes controlling the moment it starts,
        before C1a ends.

        The resulting path is C1a, C2b, C1b.
        """
        c1a = _sentence("C1a", start_date=date(2024, 1, 1), max_term=Term(years=2))
        c1b = _sentence(
            "C1b",
            parent_sentence_id="C1a",
            max_term=Term(years=3),
        )

        c2a = _sentence("C2a", start_date=date(2024, 1, 1), max_term=Term(years=1))
        c2b = _sentence(
            "C2b",
            parent_sentence_id="C2a",
            max_term=Term(years=2),
        )

        graph = SentenceGraph([c1a, c1b, c2a, c2b])

        result = graph.longest_path(engine=_NO_ADJUSTMENT_ENGINE)

        assert result is not None
        self.assertEqual(result.path_sentence_ids, ["C1a", "C2b", "C1b"])

    def test_chains_starting_later_become_active(self) -> None:
        """A sentence on a separate, later-starting chain can still become
        controlling as soon as it starts.

        Chain 1
         - C1a (1 year)
         - C1b (3 years)
         - C1c (2 years)

        Chain 2: C2a (5 years), starting two years after C1a's start date.

        C2a becomes active in the middle of C1b, even though no sentence in
        its chain is active at the time C1a starts.

        The resulting path is C1a, C1b, C2a.
        """
        c1a = _sentence("C1a", start_date=date(2024, 1, 1), max_term=Term(years=1))
        c1b = _sentence(
            "C1b",
            parent_sentence_id="C1a",
            max_term=Term(years=3),
        )
        c1c = _sentence(
            "C1c",
            parent_sentence_id="C1b",
            max_term=Term(years=2),
        )

        c2a = _sentence("C2a", start_date=date(2026, 1, 1), max_term=Term(years=5))

        graph = SentenceGraph([c1a, c1b, c1c, c2a])

        result = graph.longest_path(engine=_NO_ADJUSTMENT_ENGINE)

        assert result is not None
        self.assertEqual(result.path_sentence_ids, ["C1a", "C1b", "C2a"])

    def test_later_chain_becomes_controlling_on_its_own_start_date(self) -> None:
        """C1a (1 year) is the only sentence active on the earliest start date.
        C2a, on a separate chain, starts halfway through C1a. C1b is
        consecutive to C1a.

        C2a ends later than C1a, so C2a becomes controlling as soon as it starts.
        C1b starts on in the middle C2a and ends later than C2a, so C1b takes over
        from C2a.

        C2a must become controlling on its own start date, not only when C1a
        ends. Waiting until C1a ends would mean C2a and C1b become active at
        the same time, and C2a would be skipped because C1b ends later.

        The resulting path is C1a, C2a, C1b.
        """
        c1a = _sentence("C1a", start_date=date(2024, 1, 1), max_term=Term(years=1))
        c1b = _sentence(
            "C1b",
            parent_sentence_id="C1a",
            max_term=Term(years=5),
        )

        c2a = _sentence("C2a", start_date=date(2024, 7, 1), max_term=Term(years=3))

        graph = SentenceGraph([c1a, c1b, c2a])

        result = graph.longest_path(engine=_NO_ADJUSTMENT_ENGINE)

        assert result is not None
        self.assertEqual(result.path_sentence_ids, ["C1a", "C2a", "C1b"])

    def test_each_later_chain_becomes_controlling_when_it_starts(self) -> None:
        """Two separate chains both start partway through C1a, at different
        times. Each one takes over as it starts.

        C1a (one year) is the only sentence active on the earliest start date.
        C2a starts midway through C1a and runs two years.
        C3a starts midway through both other chains and runs five years.

        The resulting path is C1a, C2a, C3a.

        This test might catch regressions in handling multiple chains starting before
        any sentence ends.
        """
        c1a = _sentence("C1a", start_date=date(2024, 1, 1), max_term=Term(years=1))

        c2a = _sentence("C2a", start_date=date(2024, 4, 1), max_term=Term(years=2))

        c3a = _sentence("C3a", start_date=date(2024, 7, 1), max_term=Term(years=5))

        graph = SentenceGraph([c1a, c2a, c3a])

        result = graph.longest_path(engine=_NO_ADJUSTMENT_ENGINE)

        assert result is not None
        self.assertEqual(result.path_sentence_ids, ["C1a", "C2a", "C3a"])

    def test_branch_only_credits_the_longer_sibling(self) -> None:
        """C1a branches into two consecutive sentences, C1b and C1c.

        C1b (2 years) and C1c (5 years) start at the same time, so neither
        precedes the other. Only C1c will be a controlling sentence.

        The result must not depend on which order the sentences are passed to
        SentenceGraph, since C1b and C1c start simultaneously either way.
        """
        c1a = _sentence("C1a", start_date=date(2024, 1, 1), max_term=Term(years=1))
        c1b = _sentence(
            "C1b",
            parent_sentence_id="C1a",
            max_term=Term(years=2),
        )
        c1c = _sentence(
            "C1c",
            parent_sentence_id="C1a",
            max_term=Term(years=5),
        )

        shorter_sibling_first = SentenceGraph([c1a, c1b, c1c]).longest_path(
            engine=_NO_ADJUSTMENT_ENGINE
        )
        longer_sibling_first = SentenceGraph([c1a, c1c, c1b]).longest_path(
            engine=_NO_ADJUSTMENT_ENGINE
        )

        assert shorter_sibling_first is not None
        assert longer_sibling_first is not None
        self.assertEqual(shorter_sibling_first.path_sentence_ids, ["C1a", "C1c"])
        self.assertEqual(longer_sibling_first.path_sentence_ids, ["C1a", "C1c"])

    def test_sentences_in_the_middle_of_different_chains_can_start_together(
        self,
    ) -> None:
        """Two chains start at the same time. C1a and C2a are the same length,
        so they also end at the same time, which means their successors, C1b
        and C2b, also start together.
        This test checks that sentences with the same start date in the middle
        of ongoing chains are correctly evaluated.

        Chain 1:
         - C1a (1 year)
         - C1b (3 years)
         - C1c (2 years)

        Chain 2:
         - C2a (1 year)
         - C2b (4 years)

        C1a and C2a have the same end date, so C1b and C2b tie to start first. (C1a is
        controlling because it has a lower sentence id.) C2b ends later than C1b, so it
        becomes the next controlling sentence. Once C1b ends, C1c starts and ends later
        than C2b, so it takes over.

        The resulting path is C1a, C2b, C1c.
        """
        # C1a and C2a tie on end date, category, and group, so the lower
        # sentence ID breaks the tie.
        c1a = _sentence(
            "C1a",
            start_date=date(2024, 1, 1),
            max_term=Term(years=1),
            offense_category=NevadaOffenseCategory.CATEGORY_C,
            offense_group=NevadaOffenseGroup.OTHER,
        )
        c1b = _sentence(
            "C1b",
            parent_sentence_id="C1a",
            max_term=Term(years=3),
        )
        c1c = _sentence(
            "C1c",
            parent_sentence_id="C1b",
            max_term=Term(years=2),
        )

        c2a = _sentence(
            "C2a",
            start_date=date(2024, 1, 1),
            max_term=Term(years=1),
            offense_category=NevadaOffenseCategory.CATEGORY_C,
            offense_group=NevadaOffenseGroup.OTHER,
        )
        c2b = _sentence(
            "C2b",
            parent_sentence_id="C2a",
            max_term=Term(years=4),
        )

        graph = SentenceGraph([c1a, c1b, c1c, c2a, c2b])

        result = graph.longest_path(engine=_NO_ADJUSTMENT_ENGINE)

        assert result is not None
        self.assertEqual(result.path_sentence_ids, ["C1a", "C2b", "C1c"])

    def test_a_later_chain_is_compared_to_a_sibling_starting_at_the_same_time(
        self,
    ) -> None:
        """This test verifies that if a later-starting chain starts at the same time
        a consecutive sentence on another chain does, the correct controlling sentence
        is chosen.

        Chain 1: C1a (1 year), starting when C2b does.

        Chain 2:
         - C2a (2 years)
         - C2b (3 years)

        C1a and C2b both start the day after C2a ends (though only C2b is consecutive
        to it).
        C2b ends later than C1a, so C1a should never control.

        The resulting path should be C2a, C2b.
        """
        c2a = _sentence("C2a", start_date=date(2025, 1, 1), max_term=Term(years=2))
        c2b = _sentence(
            "C2b",
            parent_sentence_id="C2a",
            max_term=Term(years=3),
        )

        c1a = _sentence("C1a", start_date=date(2027, 1, 1), max_term=Term(years=1))

        graph = SentenceGraph([c2a, c2b, c1a])

        result = graph.longest_path(engine=_NO_ADJUSTMENT_ENGINE)

        assert result is not None
        self.assertEqual(result.path_sentence_ids, ["C2a", "C2b"])

    def test_sentences_ending_a_day_apart_are_not_treated_as_simultaneous(
        self,
    ) -> None:
        """Two chains start at the same time. C2a is one day longer than C1a,
        so they end on different days and must not be treated as ending at
        the same moment.

        Chain 1:
         - C1a (365 days)
         - C1b (3 years)

        Chain 2:
         - C2a (366 days)
         - C2b (5 years)

        C2a starts as the controlling sentence, since it's a day longer than
        C1a. When C1a ends, C1b starts while C2a is still active, and C1b
        ends after C2a does, so C1b takes over before C2a even ends. Only
        once C1b ends and C2a's successor C2b starts does C2b, which reaches
        further still, take over from C1b.

        The resulting path is C2a, C1b, C2b.
        """
        c1a = _sentence("C1a", start_date=date(2024, 1, 1), max_term=Term(days=365))
        c1b = _sentence(
            "C1b",
            parent_sentence_id="C1a",
            max_term=Term(years=3),
        )

        c2a = _sentence("C2a", start_date=date(2024, 1, 1), max_term=Term(days=366))
        c2b = _sentence(
            "C2b",
            parent_sentence_id="C2a",
            max_term=Term(years=5),
        )

        graph = SentenceGraph([c1a, c1b, c2a, c2b])

        result = graph.longest_path(engine=_NO_ADJUSTMENT_ENGINE)

        assert result is not None
        self.assertEqual(result.path_sentence_ids, ["C2a", "C1b", "C2b"])

    def test_later_chain_sharing_start_date_with_sentence_in_chain_and_chain_controls(
        self,
    ) -> None:
        """This test targets potential regressions around handling sentence starts.
        The start date of a consecutive sentence is the end of its preceding sentence
        plus one day, but new chains start ON their sentence begin date. We need to
        consider new active sentences when either a new chain starts or any sentence
        expires: but in only the latter case should the "date of change" be extended
        by one day.

        Chain 1:
         - C1a (short)
         - C1b (long), consecutive to C1a

        Chain 2:
         - C2a (medium), starting the same day C1b does

        C1b and C2a both start the day after C1a ends. C1b ends later, so C2a
        should never control.

        The resulting path should be C1a, C1b.
        """
        c1a = _sentence("C1a", start_date=date(2024, 1, 1), max_term=Term(days=100))
        c1b = _sentence(
            "C1b",
            parent_sentence_id="C1a",
            max_term=Term(days=300),
        )

        c2a = _sentence("C2a", start_date=date(2024, 4, 10), max_term=Term(days=150))

        graph = SentenceGraph([c1a, c1b, c2a])

        result = graph.longest_path(engine=_NO_ADJUSTMENT_ENGINE)

        assert result is not None
        self.assertEqual(result.path_sentence_ids, ["C1a", "C1b"])

    def test_more_severe_sentence_ending_on_same_date_becomes_controlling(
        self,
    ) -> None:
        """Two chains start at the same time and end on the same date.

        Chain 1: C1a (5 years, category C)
        Chain 2:
         - C2a (2 years)
         - C2b (3 years, category A)

        C1a controls at first because it ends later than C2a. When C2a ends, C2b starts
        and ends on the same date as C1a. C2b is more severe than C1a, so C2b becomes
        the controlling sentence.

        The resulting path is C1a, C2b.
        """
        c1a = _sentence(
            "C1a",
            start_date=date(2024, 1, 1),
            max_term=Term(years=5),
            offense_category=NevadaOffenseCategory.CATEGORY_C,
            offense_group=NevadaOffenseGroup.OTHER,
        )

        c2a = _sentence("C2a", start_date=date(2024, 1, 1), max_term=Term(years=2))
        c2b = _sentence(
            "C2b",
            parent_sentence_id="C2a",
            max_term=Term(years=3),
            offense_category=NevadaOffenseCategory.CATEGORY_A,
            offense_group=NevadaOffenseGroup.OTHER,
        )

        graph = SentenceGraph([c1a, c2a, c2b])

        result = graph.longest_path(engine=_NO_ADJUSTMENT_ENGINE)

        assert result is not None
        self.assertEqual(result.path_sentence_ids, ["C1a", "C2b"])

    def test_consecutive_start_date_derives_from_parent_end_date(self) -> None:
        graph = SentenceGraph(
            [
                _sentence(
                    "S1",
                    start_date=date(2024, 1, 1),
                    min_term=Term(days=50),
                    max_term=Term(days=100),
                ),
                _sentence(
                    "S2",
                    parent_sentence_id="S1",
                    min_term=Term(days=20),
                    max_term=Term(days=40),
                ),
            ]
        )

        result = graph.longest_path(engine=_NO_ADJUSTMENT_ENGINE)

        assert result is not None
        self.assertEqual(
            {
                "label": "S2.start_date",
                "value": "2024-04-10",
                "formula": "S1.end_date + 1 day",
                "derived_from": [
                    {
                        "label": "S1.end_date",
                        "value": "2024-04-09",
                        "formula": "S1.start_date + S1.adjusted_max_days - 1 day",
                        "derived_from": [
                            {"label": "S1.start_date", "value": "2024-01-01"},
                            {
                                "label": "S1.adjusted_max_days",
                                "value": 100,
                                "formula": (
                                    "max(0, 100 - 0) = 100 "
                                    "(S1.term_multiplier not applied)"
                                ),
                                "derived_from": [
                                    {
                                        "label": "S1.original_max_days",
                                        "value": 100,
                                        "formula": (
                                            "0y × 365.0 + 0m × 30.0 + 100d = 100 days"
                                        ),
                                        "derived_from": [
                                            {
                                                "label": "S1.original_max_days.years",
                                                "value": 0,
                                            },
                                            {
                                                "label": "S1.original_max_days.months",
                                                "value": 0,
                                            },
                                            {
                                                "label": "S1.original_max_days.days",
                                                "value": 100,
                                            },
                                        ],
                                    },
                                    {
                                        "label": "S1.term_multiplier",
                                        "value": 1.0,
                                        "formula": (
                                            "No term_multipliers rule; default 1.0"
                                        ),
                                    },
                                    {
                                        "label": "S1.jail_credit",
                                        "value": 0,
                                        "formula": (
                                            "0y × 365.0 + 0m × 30.0 + 0d = 0 days"
                                        ),
                                        "derived_from": [
                                            {
                                                "label": "S1.jail_credit.years",
                                                "value": 0,
                                            },
                                            {
                                                "label": "S1.jail_credit.months",
                                                "value": 0,
                                            },
                                            {
                                                "label": "S1.jail_credit.days",
                                                "value": 0,
                                            },
                                        ],
                                    },
                                ],
                            },
                        ],
                    },
                ],
            },
            result.sentence_terms["S2"].start_date.to_derivation_tree(),
        )

    def test_consecutive_sentence_terms_count_from_traced_start_date(self) -> None:
        """With calendar conversion, a consecutive sentence's terms count from the
        start date traced through its parent's term, so the term derivations show
        where that start date came from."""
        graph = SentenceGraph(
            [
                _sentence(
                    "S1",
                    start_date=date(2024, 1, 1),
                    min_term=Term(months=2),
                    max_term=Term(months=4),
                ),
                _sentence(
                    "S2",
                    parent_sentence_id="S1",
                    min_term=Term(months=1),
                    max_term=Term(months=3),
                ),
            ]
        )

        result = graph.longest_path(engine=_CALENDAR_NO_ADJUSTMENT_ENGINE)

        assert result is not None
        s2_terms = result.sentence_terms["S2"]
        # S2's original max term is derived from its years, months, days, and the
        # RETRO date it counts from, which is derived from its start date and jail
        # credit.
        [_, _, _, s2_retro_date] = s2_terms.adjustment.original_max_days.sources
        [s2_retro_start_date, _] = s2_retro_date.sources
        self.assertEqual(s2_terms.start_date, s2_retro_start_date)
        self.assertEqual("S1.end_date + 1 day", s2_retro_start_date.formula)

    def test_max_ped_for_single_sentence_is_start_date_plus_min_term(self) -> None:
        graph = SentenceGraph(
            [
                _sentence(
                    "S1",
                    start_date=date(2024, 1, 1),
                    min_term=Term(days=40),
                    max_term=Term(days=100),
                )
            ]
        )

        result = graph.longest_path(engine=_NO_ADJUSTMENT_ENGINE)

        assert result is not None
        self.assertEqual(
            result.max_ped.to_derivation_tree(),
            {
                "label": "S1.ped",
                "value": "2024-02-09",
                "formula": "S1.start_date + S1.adjusted_min_days - 1 day",
                "derived_from": [
                    {"label": "S1.start_date", "value": "2024-01-01"},
                    {
                        "label": "S1.adjusted_min_days",
                        "value": 40,
                        "formula": (
                            "max(0, 40 - 0) = 40 (S1.term_multiplier not applied)"
                        ),
                        "derived_from": [
                            {
                                "label": "S1.original_min_days",
                                "value": 40,
                                "formula": "0y × 365.0 + 0m × 30.0 + 40d = 40 days",
                                "derived_from": [
                                    {"label": "S1.original_min_days.years", "value": 0},
                                    {
                                        "label": "S1.original_min_days.months",
                                        "value": 0,
                                    },
                                    {"label": "S1.original_min_days.days", "value": 40},
                                ],
                            },
                            {
                                "label": "S1.term_multiplier",
                                "value": 1.0,
                                "formula": ("No term_multipliers rule; default 1.0"),
                            },
                            {
                                "label": "S1.jail_credit",
                                "value": 0,
                                "formula": "0y × 365.0 + 0m × 30.0 + 0d = 0 days",
                                "derived_from": [
                                    {"label": "S1.jail_credit.years", "value": 0},
                                    {"label": "S1.jail_credit.months", "value": 0},
                                    {"label": "S1.jail_credit.days", "value": 0},
                                ],
                            },
                        ],
                    },
                ],
            },
        )

    def test_max_ped_can_be_from_sentence_never_controlling_max(self) -> None:
        """Same chains as test_chains_starting_later_become_active, with min
        terms added.

        Chain 1
         - C1a (1 year)
         - C1b (3 years)
         - C1c (2 years)

        Chain 2: C2a (5 years), starting two years after C1a's start date.

        C2a becomes active in the middle of C1b, even though no sentence in its chain
        is active at the time C1a starts.
        The controlling sentences are C1a, C1b, then C2a.

        C1c never controls the MAX, but it has the latest PED, so max_ped is C1c's
        start date plus its min term.
        """
        c1a = _sentence(
            "C1a",
            start_date=date(2024, 1, 1),
            min_term=Term(days=100),
            max_term=Term(years=1),
        )
        c1b = _sentence(
            "C1b",
            parent_sentence_id="C1a",
            min_term=Term(days=200),
            max_term=Term(years=3),
        )
        c1c = _sentence(
            "C1c",
            parent_sentence_id="C1b",
            min_term=Term(days=400),
            max_term=Term(years=2),
        )

        c2a = _sentence(
            "C2a",
            start_date=date(2026, 1, 1),
            min_term=Term(days=1000),
            max_term=Term(years=5),
        )

        graph = SentenceGraph([c1a, c1b, c1c, c2a])

        result = graph.longest_path(engine=_NO_ADJUSTMENT_ENGINE)

        assert result is not None
        self.assertEqual(result.path_sentence_ids, ["C1a", "C1b", "C2a"])
        # The full shape of a PED's derivation tree is checked in
        # test_max_ped_for_single_sentence_is_start_date_plus_min_term.
        self.assertEqual("C1c.ped", result.max_ped.label)
        self.assertEqual(date(2029, 2, 2), result.max_ped.value)
        # The full shape of a consecutive sentence's start date derivation tree is
        # checked in test_consecutive_start_date_derives_from_parent_end_date.
        self.assertEqual(
            "C1c.start_date", result.sentence_terms["C1c"].start_date.label
        )
        # C1b starts 2024-12-31, the day after C1a ends, and runs 1095 days, through
        # 2027-12-30.
        self.assertEqual(
            date(2027, 12, 31), result.sentence_terms["C1c"].start_date.value
        )

    def test_max_ped_from_only_sentence_on_late_starting_chain(self) -> None:
        """Chain 1: C1a (3 years), starting 2024-01-01.

        Chain 2: C2a (1 year), starting a year after C1a. C2a is the only sentence on
        its chain.

        C2a ends before C1a, so C1a controls the MAX the whole time. C2a has the
        latest PED, so max_ped is C2a's start date plus its min term.
        """
        c1a = _sentence(
            "C1a",
            start_date=date(2024, 1, 1),
            min_term=Term(days=100),
            max_term=Term(years=3),
        )
        c2a = _sentence(
            "C2a",
            start_date=date(2025, 1, 1),
            min_term=Term(days=500),
            max_term=Term(years=1),
        )

        graph = SentenceGraph([c1a, c2a])

        result = graph.longest_path(engine=_NO_ADJUSTMENT_ENGINE)

        assert result is not None
        self.assertEqual(result.path_sentence_ids, ["C1a"])
        self.assertEqual("C2a.ped", result.max_ped.label)
        # The last day of C2a's 500 day min term, counting its start date as day one.
        self.assertEqual(date(2026, 5, 15), result.max_ped.value)


class TestSentenceGraphAdjustments(unittest.TestCase):
    """Tests that SentenceGraph.longest_path uses the regime's adjusted terms."""

    def test_consecutive_sentence_starts_after_parent_adjusted_end(self) -> None:
        engine = RuleEngine(
            LawRegime(
                name="HALF_TERMS",
                description="Regime that halves every term",
                effective_date=date(2020, 1, 1),
                use_calendar_term_conversion=False,
                days_per_month=30.0,
                days_per_year=365.0,
                term_multipliers=[
                    TermMultiplierRule(
                        name="half_term_reduction",
                        description="50% reduction on both terms",
                        when=None,
                        multiplier=0.5,
                        applies_to=[
                            ReductionTarget(term=TermType.MAX_TERM, when=None),
                            ReductionTarget(term=TermType.MIN_TERM, when=None),
                        ],
                        statute=None,
                    )
                ],
                credit_rates=[],
                credit_caps=[],
                projected_dates={},
            )
        )
        s1 = _sentence(
            "S1",
            start_date=date(2024, 1, 1),
            min_term=Term(days=100),
            max_term=Term(days=200),
        )
        s2 = _sentence(
            "S2",
            parent_sentence_id="S1",
            min_term=Term(days=60),
            max_term=Term(days=80),
        )

        result = SentenceGraph([s1, s2]).longest_path(engine=engine)

        assert result is not None
        self.assertEqual(["S1", "S2"], result.path_sentence_ids)
        # 100 + 40 adjusted days.
        self.assertEqual(140, result.total_days.value)
        # S1 runs 100 adjusted days, 2024-01-01 through 2024-04-09.
        self.assertEqual(date(2024, 4, 9), result.sentence_terms["S1"].end_date.value)
        self.assertEqual(
            date(2024, 4, 10), result.sentence_terms["S2"].start_date.value
        )
        # The last day of S2's 30 adjusted min days, counting its start date as day one.
        self.assertEqual(date(2024, 5, 9), result.max_ped.value)

    def test_root_jail_credit_moves_consecutive_sentence_start_date(self) -> None:
        s1 = _sentence(
            "S1",
            start_date=date(2024, 1, 1),
            min_term=Term(days=50),
            max_term=Term(days=100),
            initial_time_served=Term(days=30),
        )
        s2 = _sentence(
            "S2",
            parent_sentence_id="S1",
            min_term=Term(days=20),
            max_term=Term(days=40),
        )

        result = SentenceGraph([s1, s2]).longest_path(engine=_NO_ADJUSTMENT_ENGINE)

        assert result is not None
        self.assertEqual(["S1", "S2"], result.path_sentence_ids)
        # S1's 100 day max term less 30 days of jail credit, plus S2's full 40 days.
        self.assertEqual(110, result.total_days.value)
        # S1 runs 70 days, 2024-01-01 through 2024-03-10.
        self.assertEqual(
            date(2024, 3, 11), result.sentence_terms["S2"].start_date.value
        )

    def test_calendar_conversion_counts_from_inherited_start_date(self) -> None:
        engine = RuleEngine(
            LawRegime(
                name="CALENDAR",
                description="Regime that converts terms using the calendar",
                effective_date=date(2020, 1, 1),
                use_calendar_term_conversion=True,
                days_per_month=30.0,
                days_per_year=365.0,
                term_multipliers=[],
                credit_rates=[],
                credit_caps=[],
                projected_dates={},
            )
        )
        s1 = _sentence(
            "S1",
            start_date=date(2024, 1, 1),
            min_term=Term(days=31),
            max_term=Term(days=31),
        )
        s2 = _sentence(
            "S2",
            parent_sentence_id="S1",
            min_term=Term(months=1),
            max_term=Term(months=1),
        )

        result = SentenceGraph([s1, s2]).longest_path(engine=engine)

        assert result is not None
        self.assertEqual(date(2024, 2, 1), result.sentence_terms["S2"].start_date.value)
        # One month from 2024-02-01 is 29 days, since 2024 is a leap year.
        self.assertEqual(29, result.sentence_terms["S2"].total_days.value)


if __name__ == "__main__":
    unittest.main()
