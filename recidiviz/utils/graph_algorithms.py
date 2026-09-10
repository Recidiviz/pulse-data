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
"""Generic graph algorithms operating on adjacency maps."""

from typing import TYPE_CHECKING, Mapping, TypeVar

# NodeT is bounded by SupportsRichComparison so the min() call in
# _canonicalize_cycle type-checks. That protocol is stubs-only, so the bound
# must be TYPE_CHECKING-guarded and falls back to an unbounded TypeVar at runtime.
if TYPE_CHECKING:
    from _typeshed import SupportsRichComparison

    NodeT = TypeVar("NodeT", bound=SupportsRichComparison)
else:
    NodeT = TypeVar("NodeT")


def _canonicalize_cycle(closing_path: list[NodeT]) -> list[NodeT]:
    """Turns a raw depth-first path that closed a cycle into the cycle itself,
    rotated to start at its smallest node.

    The input runs from the traversal entry point to the node that closes the loop,
    repeating that node at the end (e.g. [a, b, c, b]); everything before the first
    occurrence of the closing node is the tail leading into the cycle, not part of
    it. The output is the loop alone with its smallest node first, repeated at the
    end (e.g. [b, c, b]), so it is identical for a given cycle no matter where the
    traversal entered.
    """
    closing_node = closing_path[-1]
    loop = closing_path[closing_path.index(closing_node) : -1]
    min_index = loop.index(min(loop))
    rotated = loop[min_index:] + loop[:min_index]
    return [*rotated, rotated[0]]


def find_cycle(edges: Mapping[NodeT, set[NodeT]]) -> list[NodeT] | None:
    """Returns a cycle in the directed graph defined by edges (node to its
    successors), or None if the graph is acyclic.

    The returned path is the cycle rotated to start at its smallest node and
    repeating that node at the end, a canonical form stable for a given cycle
    regardless of traversal order. For example, edges {"c": {"b"}, "b": {"a"},
    "a": {"c"}} describe the cycle c -> b -> a -> c and return ["a", "c", "b", "a"].
    When the graph has more than one cycle, which one is returned is unspecified.

    The traversal is recursive, so its depth is bounded by the longest simple path
    in the graph. Not suitable for graphs with chains longer than the interpreter's
    recursion limit (~1000 by default).
    """
    visiting: set[NodeT] = set()
    visited: set[NodeT] = set()
    path: list[NodeT] = []

    def visit(node: NodeT) -> list[NodeT] | None:
        visiting.add(node)
        path.append(node)
        for successor in edges.get(node, set()):
            if successor in visiting:
                return [*path, successor]
            if successor not in visited and (cycle := visit(successor)) is not None:
                return cycle
        visiting.discard(node)
        visited.add(node)
        path.pop()
        return None

    for node in edges:
        if node not in visited and (closing_path := visit(node)) is not None:
            return _canonicalize_cycle(closing_path)
    return None
