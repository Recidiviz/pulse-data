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
import * as dagre from "@dagrejs/dagre";
import { Edge as RFEdge, Node as RFNode } from "@xyflow/react";

import {
  GraphDisplayNode,
  NodePosition,
  NodeUrn,
} from "../../../LineageStore/types";
import { LayoutEngine } from "./LayoutEngine";

export class DagreEngine implements LayoutEngine {
  // eslint-disable-next-line class-methods-use-this
  layout = async (
    nodes: RFNode<GraphDisplayNode>[],
    edges: RFEdge[],
    nodeToAnchor?: RFNode<GraphDisplayNode>
  ): Promise<Map<NodeUrn, NodePosition>> => {
    const g = new dagre.graphlib.Graph({
      directed: true,
      multigraph: false,
      compound: false,
    });

    g.setGraph({ rankdir: "LR", ranksep: 400, ranker: "tight-tree" });

    // TODO(#36174) play around w adding ranks here based on what we know about the
    // full graph, not just the graph we have here?
    nodes.map((node) =>
      g.setNode(node.id, {
        width: node.width,
        height: node.height,
      })
    );
    edges.map((edge) => g.setEdge(edge.source, edge.target, {}));

    dagre.layout(g, {});

    if (g.nodeCount() === 0) {
      return new Map();
    }

    // if there's no anchor node, just return the newly calculated positions as is
    if (nodeToAnchor === undefined) {
      return new Map(
        g.nodes().map((nodeId) => {
          const dagreNode = g.node(nodeId);
          return [
            nodeId,
            {
              x: dagreNode.x - dagreNode.width / 2,
              y: dagreNode.y - dagreNode.height / 2,
            },
          ];
        })
      );
    }

    // build offset to shift all calculated positions by such that the graph moves around
    // the node we expanded the node itself doesn't move
    const newNodePosition = g.node(nodeToAnchor.id);
    const offset = {
      x:
        nodeToAnchor.position.x -
        (newNodePosition.x - newNodePosition.width / 2),
      y:
        nodeToAnchor.position.y -
        (newNodePosition.y - newNodePosition.height / 2),
    };

    const newNodePositions = new Map(
      g.nodes().map((nodeId) => {
        const dagreNode = g.node(nodeId);
        return nodeId !== nodeToAnchor.id
          ? [
              nodeId,
              {
                x: offset.x + (dagreNode.x - dagreNode.width / 2),
                y: offset.y + (dagreNode.y - dagreNode.height / 2),
              },
            ]
          : [nodeToAnchor.id, nodeToAnchor.position];
      })
    );
    return newNodePositions;
  };
}
