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
"""Graph-owning class for sentence DAG construction and traversal."""
from collections.abc import Iterator, Sequence
from datetime import date, timedelta

import networkx as nx

from recidiviz.sentence_calculator.models.inputs import Sentence
from recidiviz.sentence_calculator.models.outputs import (
    DisplayGraph,
    GraphEdge,
    TrackedLongestSentencePath,
    TrackedSentenceAdjustment,
)
from recidiviz.sentence_calculator.models.tracked import TrackedValue, tracked_sum
from recidiviz.utils.types import assert_type


class SentenceGraph:
    """Owns the graph representing a collection of sentences and logic to find
    the longest path.

    There should be one instance of this class per calculation. We use
    NetworkX in order not to re-implement core graph algorithms, but it's not
    really load-bearing with the current implementation.

    We use the NetworkX graph for all calculations and create the DisplayGraph
    from it for the UI.
    """

    def __init__(self, sentences: Sequence[Sentence]) -> None:
        self._sentences_by_id = {s.sentence_id: s for s in sentences}

        # TODO(NV-9) Possible to remove NetworkX now -- in the Claude-written version
        # it did some heavy lifting, but now I only use it to find cycles and get the
        # starting nodes
        graph: nx.DiGraph = nx.DiGraph()
        edges = []
        for sentence in sentences:
            graph.add_node(sentence.sentence_id)
            if sentence.parent_sentence_id:
                graph.add_edge(sentence.parent_sentence_id, sentence.sentence_id)
                edges.append(
                    GraphEdge(
                        source=sentence.parent_sentence_id, target=sentence.sentence_id
                    )
                )
        if not nx.is_directed_acyclic_graph(graph):
            # Cycle detected - the sentence graph is invalid and requires user
            # intervention.
            # TODO(NV-4): Ensure that errors are surfaced in the UI.
            # Would be great to also specify which sentences form the cycle, or show
            # the cycle visualization (in which case the display graph should be set).
            raise ValueError("Input graph contains cycles, check for errors.")
        self._graph = graph
        self._display_graph = DisplayGraph(nodes=list(graph), edges=edges)

    @property
    def display_graph(self) -> DisplayGraph:
        return self._display_graph

    def longest_path(
        self,
        *,
        adjustments: dict[  # pylint: disable=unused-argument # TODO(NV-9)
            str, TrackedSentenceAdjustment
        ],
        should_calc_min_term: bool,
        use_calendar_days_per_month: bool,  # pylint: disable=unused-argument # TODO(NV-9)
    ) -> TrackedLongestSentencePath | None:
        """Compute the path of controlling sentences from earliest start date to latest
        end date.

        TODO(NV-9): Doesn't handle in-progess sentences yet (e.g. someone is mid-way
        through the chain)
        TODO(NV-9): Doesn't handle LIFE etc sentences yet.  Need to verify correct
        behavior for these cases.

        Assumptions:
         - No sentence can have multiple parents (i.e. be consecutive to two sentences).
           TODO(NV-10) this assumption is never checked in the code. This needs input
            validation when we hook up the route.
         - There are no temporal "gaps" not covered by a sentence. E.g. it is not
           possible to have a graph with two chains where one chain starts after
           the other ends.

        Each individual connected component in the sentence graph represents a chain
        of sentences consecutive to one another. Disconnected components represent
        concurrent sequences. However, it is not sufficient to find the longest chain,
        because chains can have different start dates.

        This algorithm is based on Nevada's process for selecting sentnces. Nevada
        chooses a sequence of "controlling" sentences, where the first controlling
        sentence is the longest one starting at the earliest start date found in the
        graph. At each subsequent step, the next controlling sentence is the longest
        sentence active at the previous sentence's end date + one day. This sentence
        could be on any sentence chain -- so it's entirely possible to have each
        controlling sentence run concurrently to the others, because they have
        different start dates.

        This algorithm uses a breadth-first traversal that keeps a pointer to each
        chain (adding a new pointer each time the graph branches). These chains
        progress so each current pointer points to a sentence active at the "current"
        sentence date, with that date moving forward as we choose each controlling
        sentence.

        Args:
            adjustments: For each sentence, calculated the "adjusted" term based on
                projected credits or a percentage reduction.
            should_calc_min_term: Whether to calculate the min or max term. For regimes
                that have a notion of min and max, this method is run twice, once for
                each term.
            use_calendar_days_per_month: Whether to use calendar days for year/month
                conversion.

        Returns:
            TrackedResult of days in the longest path and the end date of that path.
                None if the graph is empty.

        Raises:
            ValueError: If the graph contains cycles.
        """
        if not self._graph.nodes():
            return None

        # Keyed by sentence ID
        sentence_end_dates: dict[str, date] = {}
        sentence_lengths_days: dict[str, TrackedValue[int]] = {}

        # TODO(NV-9) Stubbed until a subsequent PR - the real date calc and adjustment
        # logic is a lot.
        # TODO(NV-9) Ensure jail time is added correctly in adjustments and only to the
        # correct sentences!
        # TODO(NV-17): Handle the case where there's sentences from two JOCs and
        # they have different amounts of jail time.
        def calculate_term_days_and_end_date(
            sentence_id: str, inherited_start_date: date | None
        ) -> tuple[TrackedValue[int], date]:
            # TODO(NV-9), important: Since controlling sentences might overlap, this
            # needs to calculate and return the time from the last end date to the
            # next end date, which is likely to be shorter than the total days (e.g. if
            # the sentence was on a different "setence level" and not consecutive to
            # the previous controlling sentence).
            # Determine whether we need to know (in the resulting tracked value) how
            # many days each controlling sentence contributed. If we don't, then the
            # total days will fall very neatly out of the final end date - start date,
            # and we won't need to be careful here about the intermediate intervals.
            try:
                sentence = self._sentences_by_id[sentence_id]
            except KeyError as e:
                raise ValueError(
                    f"Got a sentence ID that didn't map to a real sentence: [{sentence_id}]"
                ) from e
            term = sentence.min_term if should_calc_min_term else sentence.max_term
            # TODO(NV-9): This is wrong! but this is just a stub and this logic will
            # be implemented correctly later. What values to use for "how many days are
            # in a month/year" is configured by the rule set.
            total_days: int = (term.years * 365) + (term.months * 30) + term.days
            # The start date shouldn't be null because it should indeed use the
            # inherited start date if it doesn't have its own. This stub doesn't
            # handle that.
            non_null_inherited_start_date = assert_type(inherited_start_date, date)
            return (
                TrackedValue.input(total_days, f"{sentence_id}.total_days"),
                non_null_inherited_start_date + timedelta(total_days),
            )

        def get_sentence_end_date(sentence: Sentence) -> date:
            _, end_date = get_sentence_length_and_end_date(sentence)
            return end_date

        def get_sentence_length_and_end_date(
            sentence: Sentence,
        ) -> tuple[TrackedValue[int], date]:
            """Populates the sentence ID to sentence length/end date dicts.

            Returns the sentence length and end date -- either by retrieving them from
            the dicts or calculating and adding them to the dicts.
            """
            sid = sentence.sentence_id
            if sid in sentence_end_dates and sid in sentence_lengths_days:
                return sentence_lengths_days[sid], sentence_end_dates[sid]

            sentence_length_days, sentence_end_date = calculate_term_days_and_end_date(
                sid, sentence.start_date
            )
            sentence_end_dates[sid] = sentence_end_date
            sentence_lengths_days[sid] = sentence_length_days
            return sentence_length_days, sentence_end_date

        def get_sentences_from_ids(sentence_ids: Iterator[str]) -> list[Sentence]:
            sentences: list[Sentence] = []
            for sid in sentence_ids:
                try:
                    sentence = self._sentences_by_id[sid]
                except KeyError as e:
                    # TODO(NV-5): Log errors rather than throwing.
                    raise ValueError(
                        f"Could not find a sentence associated with sentence ID [{sid}]"
                    ) from e
                sentences.append(sentence)
            return sentences

        # Get the starting nodes of each component in the graph
        # Assumption: no node has multiple parents.
        # (If multiple parents, possible for two of these 'root nodes' to have the same child)
        root_nodes = [node for node, degree in self._graph.in_degree() if degree == 0]

        root_sentences: list[Sentence] = []

        earliest_start_date: date = date.max
        for sentence_id in root_nodes:
            try:
                sentence = self._sentences_by_id[sentence_id]
            except KeyError as e:
                # TODO(NV-5): Log errors rather than throwing.
                raise ValueError(
                    f"Could not find a sentence associated with sentence ID [{sentence_id}]"
                ) from e
            if not sentence.start_date:
                # TODO(NV-5): Log errors rather than throwing.
                raise ValueError(
                    f"No start date for root / Level 1 sentence [{sentence.sentence_id}]"
                )
            earliest_start_date = min(earliest_start_date, sentence.start_date)
            root_sentences.append(sentence)

        # The most common case is that all level 1 / root sentences have the same start date.
        # We must start with a sentence with the earliest possibles start date: but if multiple
        # sentences start on that date, we should pick the one with the latest end.

        # It is possible that some sentence chains start later than others.
        # We won't consider these sentences until the "current" date is after
        # their start date.
        active_sentences: list[Sentence] = []
        later_sentence_chains: list[tuple[Sentence, date]] = []
        for sentence in root_sentences:
            # Known non-null from check above
            start = assert_type(sentence.start_date, date)
            if start == earliest_start_date:
                active_sentences.append(sentence)
            else:
                later_sentence_chains.append((sentence, start))

        # The sort order enables us to pop sentences off the end as the "current" date
        # moves forward
        later_sentence_chains = sorted(
            later_sentence_chains, key=lambda s: s[1], reverse=True
        )

        controlling_sentence_ids: list[str] = []
        total_days = TrackedValue.input(0, "Start of sentence chain")
        while active_sentences:
            # TODO(NV-9): Learn tie behavior from NV and edit this accordingly
            # The next controlling sentence is the active sentence that ends latest
            controlling_sentence = sorted(
                active_sentences, key=get_sentence_end_date, reverse=True
            )[0]
            length_days, end_date = get_sentence_length_and_end_date(
                controlling_sentence
            )
            curr_date = end_date + timedelta(days=1)
            total_days = tracked_sum(
                [total_days, length_days],
                "Total length of time in days with new controlling sentence added",
            )
            controlling_sentence_ids.append(controlling_sentence.sentence_id)

            # Picking the max end date above guaranteed that all of these sentences
            # have completed by the time the new controlling sentence completed.
            # Get their children to check for potential next controlling sentence.
            new_sentence_chains: list[Sentence] = []
            for sentence in active_sentences:
                new_sentence_chains.extend(
                    get_sentences_from_ids(self._graph.successors(sentence.sentence_id))
                )

            # Check if any of the later sentence chains have "started" yet
            while later_sentence_chains and later_sentence_chains[-1][1] <= curr_date:
                new_sentence_chains.append(later_sentence_chains.pop()[0])

            # This isn't a heap because sentences don't implement "less than"
            # The ordering is with the earliest end dates last in the list so that we
            # can pop those elements O(1).
            new_sentence_chains.sort(key=get_sentence_end_date, reverse=True)

            # It's possible that some of the children were sentences that fully
            # elapsed during the previous controlling sentence. Remove them from the
            # list and instead consider their children, if any.
            while (
                new_sentence_chains
                and get_sentence_end_date(new_sentence_chains[-1]) < curr_date
            ):
                # This sentence fully elapsed during the previous controlling sentence
                skipped_sentence = new_sentence_chains.pop()
                new_sentence_chains.extend(
                    get_sentences_from_ids(
                        self._graph.successors(skipped_sentence.sentence_id)
                    )
                )
                new_sentence_chains.sort(key=get_sentence_end_date, reverse=True)

            # Every sentence chain pointer gets "moved along" one -- i.e. its children
            # are added to a new list that becomes the "active list".
            active_sentences = new_sentence_chains

        if later_sentence_chains:
            remaining_sentences = str(
                list(map(lambda x: x[0].sentence_id, later_sentence_chains))
            )
            # TODO(NV-5): Log errors rather than throwing.
            raise ValueError(
                "A sentence chain was never reached. Either there's a gap in the "
                f"sentences or an implementation error. Unreached chains: [{remaining_sentences}]"
            )
        return TrackedLongestSentencePath(
            path_sentence_ids=controlling_sentence_ids,
            total_days=total_days,
        )
