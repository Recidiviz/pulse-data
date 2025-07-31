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
Tests helper functions for state entities (and their normalized counterparts)
"""

import unittest

from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state.state_entity_utils import (
    ConsecutiveSentenceErrors,
    ConsecutiveSentenceGraph,
)
from recidiviz.tests.utils.test_utils import assert_group_contains_regex


class ConsecutiveSentenceGraphTest(unittest.TestCase):
    """
    Tests that topological_order put all parents first,
    but demonstrates the order of person.sentences determines the
    literal processing order amongst parents.
    """

    def setUp(self) -> None:
        self.person = state_entities.StatePerson(
            state_code=StateCode.US_XX.value,
        )
        self.sentence_A = state_entities.StateSentence(
            state_code=StateCode.US_XX.value, external_id="A"
        )
        self.sentence_B = state_entities.StateSentence(
            state_code=StateCode.US_XX.value, external_id="B"
        )
        self.sentence_C = state_entities.StateSentence(
            state_code=StateCode.US_XX.value, external_id="C"
        )
        self.person.sentences = [self.sentence_A, self.sentence_B, self.sentence_C]

    def test_topological_order_no_consecutive_sentences(self) -> None:
        graph = ConsecutiveSentenceGraph.from_person(self.person)
        assert graph.topological_order == ["A", "B", "C"]

    def test_topological_order_linear_parentage(self) -> None:
        self.sentence_A.parent_sentence_external_id_array = "B"
        self.sentence_B.parent_sentence_external_id_array = "C"
        graph = ConsecutiveSentenceGraph.from_person(self.person)
        assert graph.topological_order == ["C", "B", "A"]

        self.sentence_A.parent_sentence_external_id_array = "C"
        self.sentence_B.parent_sentence_external_id_array = None
        graph = ConsecutiveSentenceGraph.from_person(self.person)
        assert graph.topological_order == ["B", "C", "A"]

    def test_topological_order_two_parents(self) -> None:
        self.sentence_A.parent_sentence_external_id_array = "B||C"
        graph = ConsecutiveSentenceGraph.from_person(self.person)
        assert graph.topological_order == ["B", "C", "A"]

        # They array order doesn't matter, but we still get all parents first
        self.sentence_A.parent_sentence_external_id_array = "C||B"
        graph = ConsecutiveSentenceGraph.from_person(self.person)
        assert graph.topological_order == ["B", "C", "A"]

        # Should still work if two parents have a single parent
        sentence_D = state_entities.StateSentence(
            state_code=StateCode.US_XX.value, external_id="D"
        )
        self.sentence_B.parent_sentence_external_id_array = "D"
        self.sentence_C.parent_sentence_external_id_array = "D"
        self.person.sentences.append(sentence_D)
        graph = ConsecutiveSentenceGraph.from_person(self.person)
        assert graph.topological_order == ["D", "B", "C", "A"]

    def test_topological_order_two_sets_of_conscutive(self) -> None:
        sentence_D = state_entities.StateSentence(
            state_code=StateCode.US_XX.value, external_id="D"
        )
        self.sentence_A.parent_sentence_external_id_array = "C"
        self.sentence_B.parent_sentence_external_id_array = "D"
        self.person.sentences.append(sentence_D)
        graph = ConsecutiveSentenceGraph.from_person(self.person)
        assert graph.topological_order == ["C", "D", "A", "B"]

        self.person.sentences.append(
            state_entities.StateSentence(
                state_code=StateCode.US_XX.value, external_id="E"
            )
        )
        graph = ConsecutiveSentenceGraph.from_person(self.person)
        assert graph.topological_order == ["C", "D", "E", "A", "B"]

    def test_from_person_no_sentences(self) -> None:
        self.person.sentences = []
        graph = ConsecutiveSentenceGraph.from_person(self.person)
        assert len(graph.nodes) == 0
        # pylint did not like == []
        assert not graph.topological_order
        assert isinstance(graph.topological_order, list)

    def test_cycle_linear_parentage(self) -> None:
        self.sentence_A.parent_sentence_external_id_array = "B"
        self.sentence_B.parent_sentence_external_id_array = "C"
        self.sentence_C.parent_sentence_external_id_array = "A"

        # Check the exection type is correct
        with self.assertRaises(ConsecutiveSentenceErrors):
            ConsecutiveSentenceGraph.from_person(self.person)

        # Now check the sub-exceptions
        # Left out the state person repr from the regex because escaping []() was annoying
        with assert_group_contains_regex(
            "Consecutive sentence errors for StatePerson",
            [
                (
                    ValueError,
                    "has an invalid set of consecutive sentences that form a cycle: C -> as child of -> A; B -> as child of -> C; A -> as child of -> B. Did you intend to hydrate these a concurrent sentences?",
                ),
            ],
        ):
            ConsecutiveSentenceGraph.from_person(self.person)

    def test_invalid_graph_formulation(self) -> None:
        self.sentence_A.parent_sentence_external_id_array = "E"
        self.sentence_B.parent_sentence_external_id_array = "F"

        # Check the exection type is correct
        with self.assertRaises(ConsecutiveSentenceErrors):
            ConsecutiveSentenceGraph.from_person(self.person)

        # Now check the sub-exceptions
        # Left out the state person repr from the regex because escaping []() was annoying
        with assert_group_contains_regex(
            "Consecutive sentence errors for StatePerson",
            [
                (
                    ValueError,
                    "with parent sentence external ID E, but no sentence with that external ID exists.",
                ),
                (
                    ValueError,
                    "with parent sentence external ID F, but no sentence with that external ID exists.",
                ),
            ],
        ):
            ConsecutiveSentenceGraph.from_person(self.person)
