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
import ElkConstructor, {
  ELK,
  ElkNode,
  LayoutOptions,
} from "elkjs/lib/elk.bundled";

import {
  GraphDisplayNode,
  NodePosition,
  NodeUrn,
} from "../../../LineageStore/types";
import { throwExpression } from "../../../LineageStore/Utils";
import { LayoutEngine } from "./LayoutEngine";

const elkOptions = {
  "elk.algorithm": "layered",
  "elk.layered.spacing.nodeNodeBetweenLayers": "80",
  "elk.spacing.nodeNode": "60",
  "elk.direction": "RIGHT",
  "elk.edgeRouting": "ORTHOGONAL",
  "elk.padding": "[top=50,left=50,bottom=50,right=50]",
  "elk.layered.nodePlacement.strategy": "BRANDES_KOEPF",
  "elk.layered.considerModelOrder.strategy": "NODES_AND_EDGES",
};

export class ElkEngine implements LayoutEngine {
  elk: ELK;

  options: LayoutOptions;

  constructor() {
    this.elk = new ElkConstructor();
    this.options = elkOptions;
  }

  layout = async (
    nodes: Node<GraphDisplayNode>[],
    edges: Edge[]
  ): Promise<Map<NodeUrn, NodePosition>> => {
    const graph: ElkNode = {
      id: "root",
      layoutOptions: this.options,
      children: nodes.map((node) => ({
        id: node.id,
        width: node.width,
        height: node.height,
      })),
      edges: edges.map((edge) => ({
        id: edge.id,
        sources: [edge.source],
        targets: [edge.target],
      })),
    };

    const graphWithUpdatedPositions: ElkNode = await this.elk.layout(graph);

    if (!graphWithUpdatedPositions.children) {
      return new Map();
    }

    const newPositions = new Map(
      graphWithUpdatedPositions.children.map((elkNode) => {
        if (elkNode.x === undefined || elkNode.y === undefined) {
          throwExpression(`Failed to computed layout for ${elkNode.id}`);
        }
        return [elkNode.id, { x: elkNode.x, y: elkNode.y }];
      })
    );

    return newPositions;
  };
}
