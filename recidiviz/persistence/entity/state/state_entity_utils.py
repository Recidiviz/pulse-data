# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
# ============================================================================
"""
Helper functions for state entities (and their normalized counterparts)
that can be used in multiple places.
"""

import networkx

from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state import normalized_entities


class ConsecutiveSentenceErrors(ExceptionGroup):
    ...


class ConsecutiveSentenceGraph(networkx.DiGraph):
    """
    A directed graph representing consecutive sentences for a person.
    This is a subclass of networkx.DiGraph to allow for custom methods
    or properties in the future if needed.
    """

    @property
    def topological_order(self) -> list[str]:
        """
        Returns the processing order of the sentences in this graph.
        The order is determined by topological sorting.
        """
        return list(networkx.topological_sort(self))

    def show(self) -> None:
        """
        Displays the graph using networkx's built-in drawing capabilities.
        For debugging purposes.
        """
        # uses sys.stdout, so we don't call print or anything to keep mypy happy
        networkx.write_network_text(self, vertical_chains=True, ascii_only=True)

    @classmethod
    def from_person(
        cls,
        state_person: state_entities.StatePerson
        | normalized_entities.NormalizedStatePerson,
    ) -> "ConsecutiveSentenceGraph":
        """
        Builds a ConsecutiveSentenceGraph from a given person.
        Raises ConsecutiveSentenceErrors if there are issues with the sentences.
        """
        sentences_by_external_id = {s.external_id: s for s in state_person.sentences}
        graph = cls()
        graph.add_nodes_from(sentences_by_external_id.keys())
        exceptions: list[ValueError] = []

        for sentence in sentences_by_external_id.values():
            for parent_id in sentence.parent_sentence_external_ids:
                if parent_id in sentences_by_external_id:
                    # Parent then child
                    graph.add_edge(parent_id, sentence.external_id)
                else:
                    exceptions.append(
                        ValueError(
                            f"Found sentence {sentence.limited_pii_repr()} with parent "
                            f"sentence external ID {parent_id}, but no sentence with that "
                            "external ID exists."
                        )
                    )
        try:
            if cycle := networkx.find_cycle(graph, orientation="original"):
                cycle_detail_str = "; ".join(
                    f"{child} -> as child of -> {parent}" for parent, child, _ in cycle
                )
                exceptions.append(
                    ValueError(
                        f"{state_person.limited_pii_repr()} has an invalid set of consecutive sentences that form a cycle: {cycle_detail_str}. "
                        "Did you intend to hydrate these a concurrent sentences?"
                    )
                )
        except networkx.NetworkXNoCycle:
            pass
        if exceptions:
            raise ConsecutiveSentenceErrors(
                f"Consecutive sentence errors for {state_person.limited_pii_repr()}",
                exceptions,
            )
        return graph
