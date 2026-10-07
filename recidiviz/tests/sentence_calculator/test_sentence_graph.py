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

from recidiviz.sentence_calculator.engine.sentence_graph import SentenceGraph
from recidiviz.sentence_calculator.models.inputs import SentenceType, Term
from recidiviz.tests.sentence_calculator.fake_sentence import (
    FakeOffenseCategory,
    FakeOffenseGroup,
    FakeSentence,
)


def _sentence(
    sentence_id: str,
    *,
    parent_sentence_id: str | None = None,
    start_date: date | None = None,
    min_term: Term = Term(),
    max_term: Term = Term(),
) -> FakeSentence:
    # Pylint known false positive
    return FakeSentence(  # pylint: disable=abstract-class-instantiated
        sentence_id=sentence_id,
        min_term=min_term,
        max_term=max_term,
        offense_category=FakeOffenseCategory.CATEGORY_C,
        offense_group=FakeOffenseGroup.NONE,
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


class TestSentenceGraphLongestPath(unittest.TestCase):
    """Tests for SentenceGraph.longest_path()."""

    def test_empty_graph_returns_none(self) -> None:
        graph = SentenceGraph([])

        result = graph.longest_path(
            adjustments={},
            should_calc_min_term=False,
            use_calendar_days_per_month=False,
        )

        self.assertIsNone(result)

    def test_single_sentence_returns_its_own_term_length(self) -> None:
        graph = SentenceGraph(
            [_sentence("S1", start_date=date(2024, 1, 1), max_term=Term(days=100))]
        )

        result = graph.longest_path(
            adjustments={},
            should_calc_min_term=False,
            use_calendar_days_per_month=False,
        )

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

        result = graph.longest_path(
            adjustments={},
            should_calc_min_term=False,
            use_calendar_days_per_month=False,
        )

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
            graph.longest_path(
                adjustments={},
                should_calc_min_term=False,
                use_calendar_days_per_month=False,
            )

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
            start_date=date(2028, 12, 31),
            max_term=Term(years=1),
        )
        c1c = _sentence(
            "C1c",
            parent_sentence_id="C1b",
            start_date=date(2030, 1, 1),
            max_term=Term(years=1),
        )
        c1d = _sentence(
            "C1d",
            parent_sentence_id="C1c",
            start_date=date(2031, 1, 2),
            max_term=Term(years=5),
        )

        c2a = _sentence("C2a", start_date=date(2024, 1, 1), max_term=Term(years=3))
        c2b = _sentence(
            "C2b",
            parent_sentence_id="C2a",
            start_date=date(2027, 1, 1),
            max_term=Term(years=5),
        )

        graph = SentenceGraph([c1a, c1b, c1c, c1d, c2a, c2b])

        result = graph.longest_path(
            adjustments={},
            should_calc_min_term=False,
            use_calendar_days_per_month=False,
        )

        assert result is not None
        self.assertEqual(result.path_sentence_ids, ["C1a", "C2b", "C1d"])
        self.assertEqual(result.total_days.value, 5475)


if __name__ == "__main__":
    unittest.main()
