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

import { findUnreachableNodes } from "./GraphHelpers";
import { LineageRootStore } from "./LineageRootStore";
import { NodeFilter } from "./NodeFilter/NodeFilter";
import { applyFiltersToNode, buildNodeFilter } from "./NodeFilter/Utils";
import {
  BigQueryGraphDisplayNode,
  BigQueryLineageNode,
  NodeFilterKey,
  NodeFilterType,
  NodeUrn,
} from "./types";
import {
  buildSelectOptionForFilter,
  createBigQueryNodeAutoCompleteGroups,
} from "./Utils";

type NodeFilteringResult = {
  displayedNodes: Node<BigQueryGraphDisplayNode>[];
  hiddenNodes: Node<BigQueryGraphDisplayNode>[];
};

/**
 * Stores for managing visual and interactive elements layered on top of the lineage graph
 */
export class UiStore {
  // visual filters that are applied |nodes| to remove certain nodes from the graph
  nodeFilters: NodeFilter[];

  // whether or not the filter modal is currently active
  filterModalOpen: boolean;

  constructor(public rootStore: LineageRootStore) {
    this.nodeFilters = [];
    this.filterModalOpen = false;

    makeAutoObservable(
      this,
      {
        rootStore: false,
      },
      { autoBind: true }
    );
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

  // builds dataset filter options that are "valid" based on what the selected node is
  get datasetFilterOptions() {
    if (this.rootStore.graphStore.hasSelectedNode) {
      return this.allDatasetFilterOptions.filter(
        (option) =>
          !option.filter.shouldIncludeNode(
            this.rootStore.graphStore.selectedNode
          )
      );
    }
    return [];
  }

  // builds state code filter options that are "valid" based on what the selected node is
  get stateCodeFilterOptions() {
    if (this.rootStore.graphStore.hasSelectedNode) {
      if (this.rootStore.graphStore.selectedNode?.data.stateCode === null) {
        return this.allStateCodeFilterOptions;
      }
      return this.allStateCodeFilterOptions.filter((option) =>
        option.filter.shouldIncludeNode(this.rootStore.graphStore.selectedNode)
      );
    }
    return [];
  }

  get filtersActive(): boolean {
    return this.nodeFilters.length !== 0;
  }

  get includeFilters(): NodeFilter[] {
    return this.nodeFilters.filter((f) => f.type === NodeFilterType.INCLUDE);
  }

  get excludeFilters(): NodeFilter[] {
    return this.nodeFilters.filter((f) => f.type === NodeFilterType.EXCLUDE);
  }

  setFilterModalState = (open: boolean) => {
    this.filterModalOpen = open;
  };

  /**
   * Applies |newFilters| to the graph in `rootStore.graphStore`.
   *
   * If we were to just apply filters across all nodes, there's a chance some nodes are
   * no longer reachable if they strictly downstream of a node that was filtered out --
   * instead, we will start with the selectedNode and then expand outwards iteratively
   * applying filters on each level until we re-build the graph and mark any unreachable
   * nodes as hidden as well.
   */
  updateFilters = async (newFilters: NodeFilter[], selectedUrn: NodeUrn) => {
    this.nodeFilters = newFilters;
    // reset all nodes as no longer hidden
    const nodesWithHiddenReset = this.rootStore.graphStore.nodes.map((n) => ({
      ...n,
      hidden: false,
    }));

    // apply filters to figure out which nodes we will be filtering out
    const { displayedNodes, hiddenNodes } =
      this.applyFiltersToExistingNodes(nodesWithHiddenReset);

    // walk the graph to determine which, if any, nodes are "unreachable"
    const unReachableNodeUrns = findUnreachableNodes(
      selectedUrn,
      nodesWithHiddenReset,
      // we need to recompute edges from the set of all possible nodes, not just the
      // rootStore.graphStore.edges as that only contains the edges between displayed
      // nodes
      this.rootStore.lineageStore.computeEdgesFromNodes(
        nodesWithHiddenReset.map((n) => n.id)
      ),
      hiddenNodes
    );

    // mark any unreachable nodes as "hidden"
    const displayedAndReachableNodes = displayedNodes.filter((n) => {
      if (unReachableNodeUrns.has(n.id)) {
        hiddenNodes.push({ ...n, hidden: true });
        return false;
      }
      return true;
    });

    await this.rootStore.graphStore.recalculateNodePositions(
      displayedAndReachableNodes,
      hiddenNodes,
      this.rootStore.graphStore.nodeFromUrn(selectedUrn)
    );
  };

  applyFiltersToExistingNodes = (
    nodes: Node<BigQueryGraphDisplayNode>[]
  ): NodeFilteringResult => {
    if (!this.filtersActive) {
      return { displayedNodes: nodes, hiddenNodes: [] };
    }

    return nodes.reduce(
      (result: NodeFilteringResult, node: Node<BigQueryGraphDisplayNode>) => {
        const shouldDisplayNode = applyFiltersToNode(
          node,
          this.includeFilters,
          this.excludeFilters
        );

        (shouldDisplayNode ? result.displayedNodes : result.hiddenNodes).push(
          node
        );

        return result;
      },
      {
        displayedNodes: [],
        hiddenNodes: [],
      } as NodeFilteringResult
    );
  };
}
