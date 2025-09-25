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

import {
  applyEdgeChanges,
  applyNodeChanges,
  Edge,
  EdgeChange,
  EdgeSelectionChange,
  Node,
  NodeChange,
  NodeSelectionChange,
} from "@xyflow/react";
import { makeAutoObservable, runInAction } from "mobx";

import { buildNewBigQueryNodeWithDefaults } from "../components/Lineage/GraphNode/NodeBuilder";
import { DagreEngine } from "../components/Lineage/Layout/DagreEngine";
import { LayoutEngine } from "../components/Lineage/Layout/LayoutEngine";
import { LineageRootStore } from "./LineageRootStore";
import {
  BigQueryGraphDisplayNode,
  BigQueryLineageNode,
  GraphDirection,
  GraphDisplayNode,
  NodeUrn,
} from "./types";
import { throwExpression } from "./Utils";

/**
 * Store for managing the graph's state and behavior.
 */
export class GraphStore {
  // nodes that are passed to ReactFlow to render in the graph
  nodes: Node<BigQueryGraphDisplayNode>[];

  // edges that are passed to ReactFlow to render in the graph
  edges: Edge[];

  // underlying layout engine / algorithm used to re-position nodes in the viewport
  layoutEngine: LayoutEngine;

  constructor(public rootStore: LineageRootStore) {
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

  get hasNodesInGraph() {
    return this.nodes.length > 0;
  }

  /**
   * Resets the graph to be a single node.
   */
  resetGraphToUrn = (urn: NodeUrn) => {
    this.edges = [];
    this.nodes = [
      buildNewBigQueryNodeWithDefaults(
        this.rootStore.lineageStore.nodeForUrn(urn)
      ),
    ];
  };

  nodeFromUrn = (urn: NodeUrn): Node<BigQueryGraphDisplayNode> => {
    const expandedNodeIndex = this.nodes.findIndex((n) => n.id === urn);
    if (expandedNodeIndex === -1) {
      throwExpression(`Found no nodes for urn: ${urn}`);
    }
    return this.nodes[expandedNodeIndex];
  };

  /**
   * Marks a node as being expanded or not, in order to have the expansion visually
   * rendered on the frontend
   */
  markNodeAsExpanded = (
    urnToMarkExpanded: NodeUrn,
    direction: GraphDirection
  ) => {
    const node = this.nodeFromUrn(urnToMarkExpanded);

    if (direction === GraphDirection.UPSTREAM) {
      node.data = {
        ...node.data,
        isExpandedUpstream: true,
      };
    } else if (direction === GraphDirection.DOWNSTREAM) {
      node.data = {
        ...node.data,
        isExpandedDownstream: true,
      };
    }
  };

  /**
   * Adds |newNodes| to the graph and recalculates the node positions and relevant edges,
   * updating the graph nodes and edges state accordingly.
   */
  addAndRecalculateNodePositions = async (
    newNodes: BigQueryLineageNode[],
    nodeToAnchor?: Node<GraphDisplayNode>
  ) => {
    // build new list of nodes and edges
    const allNodesWithoutPositions = [
      ...this.nodes,
      ...newNodes.map((n) => buildNewBigQueryNodeWithDefaults(n)),
    ];

    const { displayedNodes, hiddenNodes } =
      this.rootStore.uiStore.applyActiveFiltersToExistingNodes(
        allNodesWithoutPositions
      );

    await this.recalculateNodePositions(
      displayedNodes,
      hiddenNodes,
      nodeToAnchor
    );
  };

  /**
   * Replaces the current nodes in the graph with |newNodes| and clears all existing
   * filters. Then, calculates node positions and computes new edges.
   */
  resetGraphWithNewNodes = async (
    newNodes: BigQueryLineageNode[],
    existingNodeToAnchor?: Node<GraphDisplayNode>,
    resetFilters?: boolean
  ) => {
    // build new list of nodes and edges
    const allNodesWithoutPositions = newNodes.map((n) =>
      buildNewBigQueryNodeWithDefaults(n)
    );

    if (resetFilters !== undefined && resetFilters) {
      this.rootStore.uiStore.clearFiltersWithoutUpdatingNodesOnTheGraph();
    }

    const { displayedNodes, hiddenNodes } =
      this.rootStore.uiStore.applyActiveFiltersToExistingNodes(
        allNodesWithoutPositions
      );

    await this.recalculateNodePositions(
      displayedNodes,
      hiddenNodes,
      existingNodeToAnchor
    );
  };

  /**
   * Recalculates the node positions and relevant edges, updating the graph nodes and
   * edges state accordingly.
   */
  recalculateNodePositions = async (
    displayedNodes: Node<BigQueryGraphDisplayNode>[],
    hiddenNodes: Node<BigQueryGraphDisplayNode>[],
    nodeToAnchor?: Node<GraphDisplayNode>
  ) => {
    const newEdges = this.rootStore.lineageStore.computeEdgesFromNodes(
      displayedNodes.map((n) => n.id)
    );

    // recalculate node positions
    const newNodePositionMap = await this.layoutEngine.layout(
      displayedNodes,
      newEdges,
      nodeToAnchor
    );

    // update state with new positions
    const newNodesWithPositions = displayedNodes.map((node) => {
      const newPosition =
        newNodePositionMap.get(node.id) ??
        throwExpression(
          `Unknown node attempted to be added to graph ${node.id}`
        );
      const { position: oldPosition, ...nonPositionContents } = node;
      return {
        ...nonPositionContents,
        position: newPosition,
        hidden: false,
      };
    });

    runInAction(() => {
      this.nodes = [
        ...newNodesWithPositions,
        ...hiddenNodes.map((n) => ({ ...n, hidden: true })),
      ];
      this.edges = newEdges;
    });
  };

  /**
   * Expands the graph to display the nodes adjacent to |urn| in |direction|.
   */
  expandGraph = async (urn: NodeUrn, direction: GraphDirection) => {
    this.markNodeAsExpanded(urn, direction);
    const nodeToExpand = this.nodeFromUrn(urn);
    const expandedNodeUrns = this.rootStore.lineageStore.expand(
      urn,
      direction,
      this.nodes
    );
    this.addAndRecalculateNodePositions(expandedNodeUrns, nodeToExpand);
  };

  // eslint-disable-next-line class-methods-use-this
  contractGraph = async (urn: NodeUrn, direction: GraphDirection) => {
    // TODO(#46345): implement contraction for mvp
  };

  /**
   * Expands the subgraph that exists between two nodes
   */
  expandSubGraph = async (
    direction: GraphDirection,
    startingUrn: NodeUrn,
    ancestorUrn: NodeUrn
  ) => {
    const nodeToExpand = this.nodeFromUrn(startingUrn);
    const graphUpdates =
      await this.rootStore.lineageStore.fetchBetweenAndAddToCurrent(
        direction,
        startingUrn,
        ancestorUrn,
        this.nodes
      );
    this.addAndRecalculateNodePositions(graphUpdates, nodeToExpand);
  };

  /**
   * Resets the graph to just be the subgraph that exists between two nodes.
   */
  resetToSubGraph = async (
    direction: GraphDirection,
    startingUrn: NodeUrn,
    ancestorUrn: NodeUrn
  ) => {
    const nodeToExpand = this.nodeFromUrn(startingUrn);
    const newNodes = await this.rootStore.lineageStore.fetchBetween(
      direction,
      startingUrn,
      ancestorUrn
    );
    this.resetGraphWithNewNodes(newNodes, nodeToExpand, true);
  };

  /**
   * Uses ReactFlow's provided applyNodeChanges update our internal representation of
   * nodes
   */
  handleReactFlowNodesChange = (
    changes: NodeChange<Node<BigQueryGraphDisplayNode>>[]
  ) => {
    this.nodes = applyNodeChanges(changes, this.nodes);
  };

  /**
   * Uses ReactFlow's provided applyEdgeChange update our internal representation of
   * edges
   */
  handleReactFlowEdgesChange = (changes: EdgeChange[]) => {
    this.edges = applyEdgeChanges(changes, this.edges);
  };

  /**
   * Highlights or de-highlights a node and its connected edges.
   */
  changeNodeHighlight = (urn: NodeUrn, selected: boolean): void => {
    const edgeChanges = this.edges
      .filter((edge) => edge.source === urn || edge.target === urn)
      .map(
        (edge) =>
          ({
            id: edge.id,
            type: "select",
            selected,
          } as EdgeSelectionChange)
      );
    runInAction(() => {
      this.handleReactFlowEdgesChange(edgeChanges);
      this.handleReactFlowNodesChange([
        {
          id: urn,
          type: "select",
          selected,
        } as NodeSelectionChange,
      ]);
    });
  };
}
