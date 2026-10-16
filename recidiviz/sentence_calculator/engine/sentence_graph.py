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
from collections.abc import Sequence
from datetime import date, timedelta

import networkx as nx

from recidiviz.sentence_calculator.models.inputs import Sentence
from recidiviz.sentence_calculator.models.outputs import (
    DisplayGraph,
    GraphEdge,
    TrackedLongestSentencePath,
    TrackedSentenceAdjustment,
)
from recidiviz.sentence_calculator.models.state_inputs.inputs_nv import (
    NvSentenceWithSortableEndDate,
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

    TODO(NV-9): This class has become uncompromisingly Nevada-specific. In a coming
    refactor, I'll break this into a state-specific implementation.
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
        graph. As soon as another sentence begins, it becomes the controlling sentence
        if it ends later than the previous controlling sentence. The previous
        controlling sentence does NOT need to have completed. This new sentence
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
            # TODO(NV-9) This logic doesn't account yet for the mandatory parole min -
            # not positive it needs to, given the 40% requirement, but should check.
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

        def get_sentence_length(sentence: Sentence) -> TrackedValue[int]:
            length, _ = get_sentence_length_and_end_date(sentence)
            return length

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

        # Get the starting nodes of each component in the graph
        # Assumption: no node has multiple parents.
        # (If multiple parents, it would be possible for two of these 'root nodes' to
        # have the same child)
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
        # In theory active_sentences could be a heap, but it makes the code less
        # readable and the practical cost of all the sorting is very low: though
        # there may be many sentences in a graph, there are unlikely to be many
        # consecutive sentences running at once.
        # TODO(NV-9): Right now this only works for Nevada sentences, because
        # NvSentenceWithSortableEndDate's tie-break requires Nevada offense categories
        # and groups, and the expiry check below always compares the controlling
        # sentence to itself. This will resolve naturally when this implementation is
        # made NV-specific, at which point this note can be removed.
        active_sentences: list[NvSentenceWithSortableEndDate] = []
        # TODO(NV-9): This is slightly cleaner as not a tuple, but just sort by end
        # date in the lambda
        later_sentence_chains: list[tuple[Sentence, date]] = []
        for sentence in root_sentences:
            # Known non-null from check above
            start = assert_type(sentence.start_date, date)
            if start == earliest_start_date:
                end = get_sentence_end_date(sentence)
                active_sentences.append(
                    NvSentenceWithSortableEndDate.from_sentence(sentence, end)
                )
            else:
                later_sentence_chains.append((sentence, start))

        # The sort order enables us to pop sentences off the end as the "current" date
        # moves forward
        later_sentence_chains.sort(key=lambda s: s[1], reverse=True)

        controlling_sentence_ids: list[str] = []
        total_days = TrackedValue.input(0, "Start of sentence chain")
        active_sentences.sort(reverse=True)
        while active_sentences:
            # The next controlling sentence is the active sentence that ends latest.
            # It stays active until the "current" date passes its end date.
            controlling_sentence = active_sentences[0]
            length_days = get_sentence_length(controlling_sentence.sentence)
            # TODO(NV-9): Add the correct number of days for each sentence.
            total_days = tracked_sum(
                [total_days, length_days],
                "Total length of time in days with new controlling sentence added",
            )
            controlling_sentence_ids.append(controlling_sentence.sentence.sentence_id)

            # Note that old controlling sentences stay in the active list until their end
            # date -- the expiring sentence pop below handles every sentence hitting its
            # expiry date.
            while (
                active_sentences
                # Checks if we need a new controlling sentence
                and active_sentences[0] <= controlling_sentence
            ):
                # Check if any of the later sentence chains have "started" yet
                # It's possible for a chain to have a start date between the current
                # date and the next sentence end date; this check ensures that such a
                # sentence can become controlling if it should.
                # TODO(NV-9): A sentence consecutive to another should start the day
                # after its parent ends, which is what next_expiration_date assumes.
                # But consecutive sentences don't yet inherit their start date from
                # their parent, so tests have to hack one in, and every test in this
                # file hacks it to equal the parent's end date instead (no gap). A
                # later chain whose start date is set to match that same hacked value
                # -- i.e. meant to represent starting on the same real day -- is
                # compared one day earlier than next_expiration_date, so it can be
                # chosen as controlling before the correctly-later sentence (the
                # parent's actual successor) is even created. Once start dates are
                # inherited correctly, this won't come up, since every later chain's
                # start date will already be correct.
                next_expiration_date = active_sentences[-1].end_date + timedelta(days=1)
                # The next event is whichever comes first: a later chain starting, or
                # the soonest-expiring active sentence's successor(s) starting. When a
                # later chain starts on the same date a sentence's successor does,
                # the two are simultaneous and must be checked together -- otherwise
                # the later chain gets compared only to the expiring sentence's own
                # end date, before its successor (which may reach further) exists.
                event_date = next_expiration_date
                if (
                    later_sentence_chains
                    # These dates aren't consecutive to anything, so we don't add a day
                    and later_sentence_chains[-1][1] < event_date
                ):
                    event_date = later_sentence_chains[-1][1]

                newly_active_sentences: list[Sentence] = []
                while active_sentences and active_sentences[-1].end_date < event_date:
                    expiring_sentence = active_sentences.pop()
                    for successor in self._graph.successors(
                        expiring_sentence.sentence.sentence_id
                    ):
                        newly_active_sentences.append(self._sentences_by_id[successor])
                while (
                    later_sentence_chains and later_sentence_chains[-1][1] == event_date
                ):
                    newly_active_sentences.append(later_sentence_chains.pop()[0])

                for sentence in newly_active_sentences:
                    end = get_sentence_end_date(sentence)
                    active_sentences.append(
                        NvSentenceWithSortableEndDate.from_sentence(sentence, end)
                    )
                active_sentences.sort(reverse=True)

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
