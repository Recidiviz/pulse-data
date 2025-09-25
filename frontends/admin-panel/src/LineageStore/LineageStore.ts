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

import { Edge } from "@xyflow/react";
import { makeAutoObservable, runInAction } from "mobx";

import { fetchNodes, fetchNodesBetween } from "../AdminPanelAPI/LineageAPI";
import { Hydratable, HydrationState } from "../InsightsStore/types";
import {
  BigQueryLineageNode,
  EdgeId,
  GraphDirection,
  GraphEdge,
  IsExpandedStatuses,
  NodeUrn,
} from "./types";
import { throwExpression } from "./Utils";

/**
 * Store for introspecting about the entire view graph.
 */
export class LineageStore implements Hydratable {
  // map of urn -> entity metadata
  nodes: Map<NodeUrn, BigQueryLineageNode>;

  // map of edge id (e-{source}-{target}) to edge
  edges: Map<EdgeId, GraphEdge>;

  // map of urn -> set of downstream nodes
  urnToDownstreamUrns: Map<NodeUrn, Set<NodeUrn>>;

  // map of urn -> set of upstream nodes
  urnToUpstreamUrns: Map<NodeUrn, Set<NodeUrn>>;

  hydrationState: HydrationState;

  constructor() {
    this.hydrationState = { status: "needs hydration" };
    this.nodes = new Map<NodeUrn, BigQueryLineageNode>();
    this.urnToDownstreamUrns = new Map<NodeUrn, Set<string>>();
    this.urnToUpstreamUrns = new Map<NodeUrn, Set<string>>();
    this.edges = new Map<EdgeId, Edge>();

    makeAutoObservable(this, undefined, { autoBind: true });
  }

  /**
   * Computes the set of valid edges that connects the provided |urns|.
   */
  computeEdgesFromNodes = (urns: Set<NodeUrn>): GraphEdge[] => {
    const candidateEdges = new Map<string, GraphEdge>();

    urns.forEach((urn: NodeUrn) => {
      // find edges between node and other active upstream nodes
      const upstreamEdgesToAdd = this.urnToUpstreamUrns
        .get(urn)
        ?.intersection(urns);
      upstreamEdgesToAdd?.forEach((upstreamUrn) => {
        const edgeId = `e-${upstreamUrn}-${urn}`;
        if (!candidateEdges.has(edgeId)) {
          candidateEdges.set(
            edgeId,
            this.edges.get(edgeId) ?? throwExpression(`Unknown edge: ${edgeId}`)
          );
        }
      });
      // find edges between node and other active downstream nodes
      const downstreamEdgesToAdd = this.urnToDownstreamUrns
        .get(urn)
        ?.intersection(urns);
      downstreamEdgesToAdd?.forEach((downstreamUrn) => {
        const edgeId = `e-${urn}-${downstreamUrn}`;
        if (!candidateEdges.has(edgeId)) {
          candidateEdges.set(
            edgeId,
            this.edges.get(edgeId) ?? throwExpression(`Unknown edge: ${edgeId}`)
          );
        }
      });
    });

    // sorts the edges so they are returned deterministically so as to avoid unnecessary
    // re-rendering (i think)?
    return Array.from(candidateEdges.values()).sort((a, b) =>
      a.id.localeCompare(b.id)
    );
  };

  computeExpandedUpstreamForNodes = (
    displayedUrns: Set<NodeUrn>,
    hiddenByFiltersUrns: Set<NodeUrn>
  ): Map<NodeUrn, IsExpandedStatuses> => {
    // combine both urns that are displayed and those that are not to determine if the
    // we have expanded all possible adj neighbors
    const allUrnsInDisplayGraph = displayedUrns.union(hiddenByFiltersUrns);
    const isExpandedMap = new Map<string, IsExpandedStatuses>();

    displayedUrns.forEach((urn: NodeUrn) => {
      // if all adj nodes for this node are in the graph, declare it expanded
      isExpandedMap.set(urn, {
        isExpandedDownstream:
          this.urnToDownstreamUrns.get(urn)?.difference(allUrnsInDisplayGraph)
            .size === 0,
        isExpandedUpstream:
          this.urnToUpstreamUrns.get(urn)?.difference(allUrnsInDisplayGraph)
            .size === 0,
      });
    });

    return isExpandedMap;
  };

  /**
   * Retrieves a set of adjacent nodes for a given node URN and direction.
   */
  adjacentNodes = (urn: NodeUrn, direction: GraphDirection): Set<NodeUrn> => {
    switch (direction) {
      case GraphDirection.UPSTREAM:
        return this.urnToUpstreamUrns.get(urn) ?? new Set();
      case GraphDirection.DOWNSTREAM:
        return this.urnToDownstreamUrns.get(urn) ?? new Set();
      default: {
        const exhaustiveCheck: never = direction;
        throwExpression(`Unhandled role: ${exhaustiveCheck}`);
      }
    }
  };

  /**
   * Expands the graph by fetching the adjacent nodes and computing the new edges to add.
   */
  expand = (
    urnToExpand: NodeUrn,
    direction: GraphDirection,
    currentUrns: Set<NodeUrn>
  ): BigQueryLineageNode[] => {
    const adjacentNodes = this.adjacentNodes(urnToExpand, direction);
    return Array.from(adjacentNodes.difference(currentUrns)).map(
      (urn) => this.nodes.get(urn) ?? throwExpression(`Unknown urn: ${urn}`)
    );
  };

  /**
   * Fetches a single node.
   */
  nodeForUrn = (urn: NodeUrn): BigQueryLineageNode => {
    return this.nodes.get(urn) ?? throwExpression(`Unknown urn: ${urn}`);
  };

  /**
   * Fetches all nodes between a start and end node.
   */
  fetchBetweenAndAddToCurrent = async (
    direction: GraphDirection,
    source: NodeUrn,
    target: NodeUrn,
    currentUrns: Set<NodeUrn>
  ): Promise<BigQueryLineageNode[]> => {
    const urnsBetween = await fetchNodesBetween(direction, source, target);
    const newNodeUrns = new Set<NodeUrn>(urnsBetween.urns);

    return Array.from(newNodeUrns.difference(currentUrns)).map(this.nodeForUrn);
  };

  fetchBetween = async (
    direction: GraphDirection,
    source: NodeUrn,
    target: NodeUrn
  ): Promise<BigQueryLineageNode[]> => {
    const urnsBetween = (await fetchNodesBetween(direction, source, target))
      .urns;
    return urnsBetween.map(this.nodeForUrn);
  };

  setHydrationState(hydrationState: HydrationState) {
    this.hydrationState = hydrationState;
  }

  /**
   * Fetches all nodes and references from the admin panel backend and populates the
   * store's internal data structures.
   */
  populateLineageStore = async () => {
    const graphResponse = await fetchNodes();

    const urnToDownstreamUrns = new Map<NodeUrn, Set<string>>();
    const urnToUpstreamUrns = new Map<NodeUrn, Set<string>>();
    const edges = new Map<EdgeId, Edge>();
    const nodes = new Map<NodeUrn, BigQueryLineageNode>();

    // use graph references to build upstream & downstream ref mapping
    graphResponse.references.forEach((r) => {
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

      // create map of edge id to edge source & target
      const edgeId = `e-${r.source}-${r.target}`;
      edges.set(edgeId, {
        id: edgeId,
        source: r.source,
        target: r.target,
      });
    });

    // create map of urn to node metadata
    graphResponse.nodes.forEach((n) =>
      nodes.set(n.urn, {
        ...n,
        hasUpstream: !!urnToUpstreamUrns.get(n.urn)?.size,
        hasDownstream: !!urnToDownstreamUrns.get(n.urn)?.size,
      })
    );
    runInAction(() => {
      this.urnToDownstreamUrns = urnToDownstreamUrns;
      this.urnToUpstreamUrns = urnToUpstreamUrns;
      this.edges = edges;
      this.nodes = nodes;
    });
  };

  hydrate = async () => {
    if (
      this.hydrationState.status === "hydrated" ||
      this.hydrationState.status === "loading"
    )
      return;
    try {
      this.setHydrationState({ status: "loading" });
      await this.populateLineageStore();
      this.setHydrationState({ status: "hydrated" });
    } catch (e) {
      this.setHydrationState({ status: "failed", error: e as Error });
    }
  };
}
