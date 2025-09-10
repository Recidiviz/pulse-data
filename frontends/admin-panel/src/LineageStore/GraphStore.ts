// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2024 Recidiviz, Inc.
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
import { makeAutoObservable, runInAction } from "mobx";

import { buildNewNodeWithDefaults } from "../components/Lineage/GraphNode/NodeBuilder";
import { DagreEngine } from "../components/Lineage/Layout/DagreEngine";
import { LayoutEngine } from "../components/Lineage/Layout/LayoutEngine";
import { LineageRootStore } from "./LineageRootStore";
import {
  GraphDirection,
  GraphDisplayNode,
  LineageNode,
  NodeUrn,
} from "./types";
import { throwExpression } from "./Utils";

/**
 * Store for managing the graph's visual state and behavior.
 */
export class GraphStore {
  // the node that is searched -- should be reflected in the url of the page
  selectedNode?: NodeUrn;

  // nodes that are passed to ReactFlow to render in the graph
  nodes: Node<GraphDisplayNode>[];

  // edges that are passed to ReactFlow to render in the graph
  edges: Edge[];

  // underlying layout engine / algorithm used to re-position nodes in the viewport
  layoutEngine: LayoutEngine;

  constructor(public rootStore: LineageRootStore) {
    this.selectedNode = undefined;
    this.nodes = [];
    this.edges = [];
    this.layoutEngine = new DagreEngine();

    makeAutoObservable(
      this,
      {
        rootStore: false,
      },
      { autoBind: true }
    );
  }

  /**
   * Resets the graph to be a single node.
   */
  resetGraphToActiveNode = async (urn: NodeUrn) => {
    this.selectedNode = urn;
    this.edges = [];
    this.nodes = [
      buildNewNodeWithDefaults(this.rootStore.lineageStore.nodeForUrn(urn)),
    ];
  };

  /**
   * Expands the graph to display the nodes adjacent to |urn| in |direction|.
   */
  expandGraph = async (urn: NodeUrn, direction: GraphDirection) => {
    this.markNodeAsExpanded(urn, direction);
    const expandedNodeUrns = this.rootStore.lineageStore.expand(
      urn,
      direction,
      this.nodes
    );
    this.recalculateNodePositions(expandedNodeUrns);
  };

  // eslint-disable-next-line class-methods-use-this
  contractGraph = async (urn: NodeUrn, direction: GraphDirection) => {
    // TODO(#46345): implement contraction for mvp
  };

  /**
   * Marks a node as being expanded or not, in order to have the expansion visually
   * rendered on the frontend
   */
  markNodeAsExpanded = (
    urnToMarkExpanded: NodeUrn,
    direction: GraphDirection
  ) => {
    const expandedNodeIndex = this.nodes.findIndex(
      (n) => n.id === urnToMarkExpanded
    );
    if (expandedNodeIndex === -1) {
      throwExpression(`Found no nodes for urn: ${urnToMarkExpanded}`);
    }

    if (direction === GraphDirection.UPSTREAM) {
      this.nodes[expandedNodeIndex].data.isExpandedUpstream = true;
    } else if (direction === GraphDirection.DOWNSTREAM) {
      this.nodes[expandedNodeIndex].data.isExpandedDownstream = true;
    }
  };

  /**
   * Given a new set of nodes |newNodes|, recalculates the nodes positions and updates
   * the graph nodes and edges state.
   */
  recalculateNodePositions = async (newNodes: LineageNode[]) => {
    // build new list of nodes and edges
    const allNodesWithoutPositions = [
      ...this.nodes,
      ...newNodes.map((n) => buildNewNodeWithDefaults(n)),
    ];

    const newEdges = this.rootStore.lineageStore.computeEdgesFromNodes(
      allNodesWithoutPositions.map((n) => n.id)
    );

    // recalculate node positions
    const newNodePositionMap = await this.layoutEngine.layout(
      allNodesWithoutPositions,
      newEdges
    );

    // update state with new positions
    const newNodesWithPositions = allNodesWithoutPositions.map((node) => {
      const newPosition =
        newNodePositionMap.get(node.id) ??
        throwExpression(
          `Unknown node attempted to be added to graph ${node.id}`
        );
      const { position: oldPosition, ...nonPositionContents } = node;
      return {
        ...nonPositionContents,
        position: newPosition,
      };
    });

    runInAction(() => {
      this.nodes = newNodesWithPositions;
      this.edges = newEdges;
    });
  };

  /**
   * Expands the subgraph that exist between two nodes
   */
  expandSubGraph = (sourceUrn: NodeUrn, targetUrn: NodeUrn) => {
    this.rootStore.lineageStore
      .fetchBetween(sourceUrn, targetUrn, this.nodes)
      .then((graphUpdates) => {
        this.recalculateNodePositions(graphUpdates);
      });
  };
}
