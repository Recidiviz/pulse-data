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

import { Node } from "@xyflow/react";
import { makeAutoObservable } from "mobx";

import { findLargestConnectedGraphOfPossibleUnconnectedGraphs } from "./GraphHelpers";
import { LineageRootStore } from "./LineageRootStore";
import { NodeFilter } from "./NodeFilter/NodeFilter";
import {
  applyFiltersToExistingNodes,
  buildNodeFilter,
  NodeFilteringResult,
} from "./NodeFilter/Utils";
import {
  BigQueryGraphDisplayNode,
  BigQueryLineageNode,
  NodeDetailDrawerTab,
  NodeFilterKey,
  NodeFilterType,
  NodeUrn,
} from "./types";
import {
  buildSelectOptionForFilter,
  createBigQueryNodeAutoCompleteGroups,
  throwExpression,
} from "./Utils";

/**
 * Stores for managing visual and interactive elements layered on top of the lineage graph
 */
export class UiStore {
  // visual filters that are applied |nodes| to remove certain nodes from the graph
  nodeFilters: NodeFilter[];

  // whether or not the filter modal is currently active
  filterModalOpen: boolean;

  // whether or not the detail drawer is open
  nodeDetailDrawerUrn?: NodeUrn;

  // which detail drawer tab is currently active; only visible if |nodeDetailDrawerUrn|
  // is non-null
  activeNodeDetailDrawerTab: NodeDetailDrawerTab;

  constructor(public rootStore: LineageRootStore) {
    this.nodeFilters = [];
    this.filterModalOpen = false;
    this.activeNodeDetailDrawerTab = NodeDetailDrawerTab.DETAILS;

    makeAutoObservable(
      this,
      {
        rootStore: false,
      },
      { autoBind: true }
    );
  }

  // whether or not the node detail drawer should be open
  get isNodeDetailDrawerOpen() {
    return this.nodeDetailDrawerUrn !== undefined;
  }

  // the corresponding node of the open urn, if one is active
  get nodeDetailDrawerNode() {
    return this.nodeDetailDrawerUrn
      ? this.rootStore.graphStore.nodeFromUrn(this.nodeDetailDrawerUrn)
      : undefined;
  }

  // builds the autocomplete options for the search bar
  get autoCompleteOptions() {
    return createBigQueryNodeAutoCompleteGroups(
      Array.from(this.rootStore.lineageStore.nodes.values())
    );
  }

  // builds one select option for each dataset id
  get allDatasetFilterOptions() {
    return Array.from(
      Array.from(this.rootStore.lineageStore.nodes.values()).reduce(
        (acc: Set<string>, node: BigQueryLineageNode) => {
          if (node.datasetId) {
            acc.add(node.datasetId);
          }
          return acc;
        },
        new Set<string>()
      )
    )
      .map((d) =>
        buildNodeFilter(
          NodeFilterType.EXCLUDE,
          NodeFilterKey.DATASET_ID_FILTER,
          d
        )
      )
      .map(buildSelectOptionForFilter);
  }

  // builds one select option for each state code
  get allStateCodeFilterOptions() {
    return Array.from(
      Array.from(this.rootStore.lineageStore.nodes.values()).reduce(
        (acc: Set<string>, node: BigQueryLineageNode) => {
          if (node.stateCode) {
            acc.add(node.stateCode);
          }
          return acc;
        },
        new Set<string>()
      )
    )
      .map((s) =>
        buildNodeFilter(
          NodeFilterType.INCLUDE,
          NodeFilterKey.STATE_CODE_FILTER,
          s
        )
      )
      .map(buildSelectOptionForFilter);
  }

  get areFiltersActive(): boolean {
    return this.nodeFilters.length !== 0;
  }

  /** IMPORTANT: while this function does clear filters, it does NOT update the nodes
   * that are displayed the graph (on purpose)
   */
  clearFiltersWithoutUpdatingNodesOnTheGraph = () => {
    this.nodeFilters = [];
  };

  setFilterModalState = (open: boolean) => {
    this.filterModalOpen = open;
  };

  setNodeDetailDrawerUrn = (urn?: NodeUrn) => {
    this.nodeDetailDrawerUrn = urn;
  };

  setActiveNodeDetailDrawerTab = (tab: NodeDetailDrawerTab) => {
    this.activeNodeDetailDrawerTab = tab;
  };

  /**
   * Applies |newFilters| to the graph in `rootStore.graphStore`.
   *
   * If we were to just apply filters across all nodes, there's a chance some nodes are
   * no longer reachable if they strictly downstream of a node that was filtered out --
   * in this case, we'd end up with an unconnected graph (which we don't want). in these
   * cases, we'll just pick the larger side of the connected graph
   * TODO(#46787) should we display a toast explaining what happened -- could be jarring
   * if we choose the larger part of the graph, but the part of the graph the user was
   * not expecting
   */
  updateFilters = async (newFilters: NodeFilter[]) => {
    // reset all nodes as no longer hidden
    const nodesWithHiddenReset = this.rootStore.graphStore.nodes.map((n) => ({
      ...n,
      hidden: false,
    }));

    // apply filters to figure out which nodes we will be filtering out
    const { displayedNodes, hiddenNodes } = applyFiltersToExistingNodes(
      nodesWithHiddenReset,
      newFilters
    );

    if (displayedNodes.length === 0) {
      throwExpression(
        "Cannot apply filters that will remove all nodes from the graph"
      );
    }

    // walk the graph to determine which, if any, nodes are "unreachable"
    const newGraphUrns = findLargestConnectedGraphOfPossibleUnconnectedGraphs(
      nodesWithHiddenReset,
      // we need to recompute edges from the set of all possible nodes, not just the
      // rootStore.graphStore.edges as that only contains the edges between displayed
      // nodes
      this.rootStore.lineageStore.computeEdgesFromNodes(
        nodesWithHiddenReset.map((n) => n.id)
      ),
      hiddenNodes
    );

    // remove any unreachable nodes from the graph
    const displayedAndReachableNodes = displayedNodes.filter((n) =>
      newGraphUrns.has(n.id)
    );

    await this.rootStore.graphStore.recalculateNodePositions(
      displayedAndReachableNodes,
      hiddenNodes,
      undefined
    );
    this.nodeFilters = newFilters;
  };

  applyActiveFiltersToExistingNodes = (
    nodes: Node<BigQueryGraphDisplayNode>[]
  ): NodeFilteringResult => {
    return applyFiltersToExistingNodes(nodes, this.nodeFilters);
  };
}
