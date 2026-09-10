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
"""Tests for find_cycle."""

import unittest

from recidiviz.utils.graph_algorithms import find_cycle


class TestFindCycle(unittest.TestCase):
    """Tests for find_cycle."""

    def test_empty_graph(self) -> None:
        self.assertIsNone(find_cycle({}))

    def test_acyclic(self) -> None:
        # a -> b -> c, a -> c (a diamond's worth of edges, no cycle).
        self.assertIsNone(find_cycle({"a": {"b", "c"}, "b": {"c"}, "c": set()}))

    def test_self_loop(self) -> None:
        # a -> a: a single node with an edge to itself.
        self.assertEqual(["a", "a"], find_cycle({"a": {"a"}}))

    def test_two_node_cycle(self) -> None:
        # a <-> b: a and b point at each other.
        self.assertEqual(["a", "b", "a"], find_cycle({"a": {"b"}, "b": {"a"}}))

    def test_returns_loop_only_not_tail_leading_into_it(self) -> None:
        # a -> b -> c -> b: the tail "a" leads into the cycle but is not part of
        # it, so the canonical result is the loop "b -> c -> b" alone.
        self.assertEqual(
            ["b", "c", "b"],
            find_cycle({"a": {"b"}, "b": {"c"}, "c": {"b"}}),
        )

    def test_result_rooted_at_min_node_regardless_of_entry_point(self) -> None:
        # The cycle b <-> c is entered from the standalone node "a", but the
        # canonical loop is rooted at its smallest node "b", not the entry point.
        self.assertEqual(
            ["b", "c", "b"],
            find_cycle({"a": {"b"}, "b": {"c"}, "c": {"b"}, "z": set()}),
        )

    def test_cycle_in_later_component_after_acyclic_root(self) -> None:
        # Two disconnected components: p -> q is acyclic and iterated first, so the
        # traversal from "p" returns no cycle and the outer loop must move on to
        # find the cycle x <-> y in the second component.
        self.assertEqual(
            ["x", "y", "x"],
            find_cycle({"p": {"q"}, "q": set(), "x": {"y"}, "y": {"x"}}),
        )

    def test_result_is_deterministic_across_set_orderings(self) -> None:
        # a branches to both the cycle node "b" and the dead-end "sink", so "a"'s
        # successor set has two elements whose iteration order is unspecified. The
        # single cycle a <-> b must canonicalize identically whichever successor
        # depth-first traversal visits first, since canonicalization no longer sorts.
        self.assertEqual(
            ["a", "b", "a"],
            find_cycle({"a": {"b", "sink"}, "b": {"a"}, "sink": set()}),
        )
        self.assertEqual(
            ["a", "b", "a"],
            find_cycle({"sink": set(), "b": {"a"}, "a": {"sink", "b"}}),
        )

    def test_result_rooted_at_min_node_when_min_is_mid_cycle(self) -> None:
        # c -> b -> a -> c: the min node "a" sits in the middle of the declared
        # path, so the canonical loop rotates to "a -> c -> b -> a".
        self.assertEqual(
            ["a", "c", "b", "a"],
            find_cycle({"c": {"b"}, "b": {"a"}, "a": {"c"}}),
        )

    def test_multiple_cycles_returns_one_valid_canonical_cycle(self) -> None:
        # a -> b -> a and a -> c -> a: a branches to b and c, both of which point
        # back to a. Which cycle is found is unspecified, but it is always one of
        # the two canonical loops.
        result = find_cycle({"a": {"b", "c"}, "b": {"a"}, "c": {"a"}})
        self.assertIn(result, (["a", "b", "a"], ["a", "c", "a"]))

    def test_missing_successor_key_treated_as_no_edges(self) -> None:
        # "b" is a successor of "a" but absent as a key; it is a valid sink.
        self.assertIsNone(find_cycle({"a": {"b"}}))
