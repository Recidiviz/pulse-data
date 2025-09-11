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
import { Edge as RFEdge, Node as RFNode } from "@xyflow/react";

import {
  GraphDisplayNode,
  NodePosition,
  NodeUrn,
} from "../../../LineageStore/types";

/**
 * Interface for different layout providers and algorithms to be used to determine the
 * visual position of nodes and edges on a canvas.
 */
export interface LayoutEngine {
  /**
   * @param {RFNode<GraphDisplayNode>[]} nodes - the nodes that we want to visually arrange
   * @param {RFEdge[]} edges - the edges that connect |nodes|
   * @param {RFNode<GraphDisplayNode> | undefined} nodeToAnchor - optional: the node to
   * anchor position calculations
   *
   * @returns {Promise<{ nodes: RFNode<GraphDisplayNode>[]; edges: RFEdge[] }>} - nodes
   * that have their x/y positions updated
   */
  layout(
    nodes: RFNode<GraphDisplayNode>[],
    edges: RFEdge[],
    nodeToAnchor?: RFNode<GraphDisplayNode>
  ): Promise<Map<NodeUrn, NodePosition>>;
}
