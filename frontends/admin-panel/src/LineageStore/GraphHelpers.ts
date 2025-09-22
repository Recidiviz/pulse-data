// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2025 Recidiviz, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.
// =============================================================================
import { Edge, Node } from "@xyflow/react";

import { BigQueryGraphDisplayNode, GraphReferenceType, NodeUrn } from "./types";

function buildAdjacencyMappingFromReferences(
  references: GraphReferenceType[]
): {
  urnToDownstreamUrns: Map<NodeUrn, Set<NodeUrn>>;
  urnToUpstreamUrns: Map<NodeUrn, Set<NodeUrn>>;
} {
  const urnToDownstreamUrns = new Map();
  const urnToUpstreamUrns = new Map();
  references.forEach((r) => {
    // downstream ref map
    if (!urnToDownstreamUrns.has(r.source)) {
      urnToDownstreamUrns.set(r.source, new Set());
    }
    urnToDownstreamUrns.get(r.source)?.add(r.target);

    // upstream ref map
    if (!urnToUpstreamUrns.has(r.target)) {
      urnToUpstreamUrns.set(r.target, new Set());
    }
    urnToUpstreamUrns.get(r.target)?.add(r.source);
  });
  return { urnToDownstreamUrns, urnToUpstreamUrns };
}

function setDifference<T>(setA: Set<T>, setB: Set<T>): Set<T> {
  const result = new Set<T>();
  setA.forEach((item) => {
    if (!setB.has(item)) {
      result.add(item);
    }
  });
  return result;
}

/**
 * Walks the graph as described by |nodes| and |edges| in order to determine which of
 * |nodes| is not reachable (unconnected) due to the removal of |nodesToFilterOut|. The
 * "main" graph is determined by which nodes are connected to |startingUrn|, not which
 * graph might be bigger. The implementation of this function is a basic BFS.
 */
export function findUnreachableNodes(
  startingUrn: NodeUrn,
  nodes: Node<BigQueryGraphDisplayNode>[],
  edges: Edge[],
  nodesToFilterOut: Node<BigQueryGraphDisplayNode>[]
): Set<NodeUrn> {
  // first, build adjacency mappings from edges and nodes
  const { urnToDownstreamUrns, urnToUpstreamUrns } =
    buildAdjacencyMappingFromReferences(edges);
  const urnsToFilterOut = new Set(nodesToFilterOut.map((n) => n.id));

  // do bfs, starting w/ startingUrn and expanding outward in all directions
  const queue: NodeUrn[] = [startingUrn];
  const visitedNodes: Set<NodeUrn> = new Set([startingUrn]);

  while (queue.length > 0) {
    const currentUrn = queue.shift();
    if (currentUrn === undefined) break;

    // peek downstream
    const downstreamNodes = setDifference(
      urnToDownstreamUrns.get(currentUrn) ?? new Set<string>(),
      urnsToFilterOut
    );
    if (downstreamNodes) {
      downstreamNodes.forEach((n) => {
        if (!visitedNodes.has(n)) {
          visitedNodes.add(n);
          queue.push(n);
        }
      });
    }

    // peek upstream
    const upstreamNodes = setDifference(
      urnToUpstreamUrns.get(currentUrn) ?? new Set<string>(),
      urnsToFilterOut
    );
    if (upstreamNodes) {
      upstreamNodes.forEach((n) => {
        if (!visitedNodes.has(n)) {
          visitedNodes.add(n);
          queue.push(n);
        }
      });
    }
  }

  const allUrns = new Set(nodes.map((n) => n.id));

  // we want to return all "unreachable" or disconnected nodes that are not already
  // going to be filtered out, so we do that by doing
  // ((allUrns - reachableUrns) - filteredOutUrns)
  return setDifference(setDifference(allUrns, visitedNodes), urnsToFilterOut);
}
