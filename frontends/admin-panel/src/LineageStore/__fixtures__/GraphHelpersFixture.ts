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

import { buildNewBigQueryNodeWithDefaults } from "../../components/Lineage/GraphNode/NodeBuilder";
import {
  BigQueryGraphDisplayNode,
  BigQueryNodeType,
  GraphType,
} from "../types";

export type TestGraphType = {
  nodes: Node<BigQueryGraphDisplayNode>[];
  edges: Edge[];
};

export function parseGraph(graph: GraphType): TestGraphType {
  return {
    nodes: graph.nodes.map((n) =>
      buildNewBigQueryNodeWithDefaults({
        ...n,
        // for graphHelpers tests so far, these properties are unused
        hasDownstream: true,
        hasUpstream: true,
      })
    ),
    edges: graph.references.map((r) => ({
      id: `e-${r.source}-${r.target}`,
      source: r.source,
      target: r.target,
    })),
  };
}

//  1     2
//   \   /
//     3
//   /   \
//  4     5
export const xShapedGraph: GraphType = {
  nodes: [
    {
      urn: "dataset_1.table_1",
      type: BigQueryNodeType.VIEW,
      datasetId: "dataset_1",
      viewId: "table_1",
      stateCode: null,
    },
    {
      urn: "dataset_2.table_2",
      type: BigQueryNodeType.VIEW,
      datasetId: "dataset_2",
      viewId: "table_2",
      stateCode: null,
    },
    {
      urn: "dataset_3.table_3",
      type: BigQueryNodeType.VIEW,
      datasetId: "dataset_3",
      viewId: "table_3",
      stateCode: null,
    },
    {
      urn: "dataset_4.table_4",
      type: BigQueryNodeType.VIEW,
      datasetId: "dataset_4",
      viewId: "table_4",
      stateCode: null,
    },
    {
      urn: "dataset_5.table_5",
      type: BigQueryNodeType.VIEW,
      datasetId: "dataset_5",
      viewId: "table_5",
      stateCode: null,
    },
  ],
  references: [
    {
      source: "dataset_1.table_1",
      target: "dataset_3.table_3",
    },
    {
      source: "dataset_2.table_2",
      target: "dataset_3.table_3",
    },
    {
      source: "dataset_3.table_3",
      target: "dataset_4.table_4",
    },
    {
      source: "dataset_3.table_3",
      target: "dataset_5.table_5",
    },
  ],
};

//  1     2
//   \   /
//  |  3  |
//   /   \
//  4     5
//   \   /
//     6
export const diamondShapedGraph: GraphType = {
  nodes: [
    {
      urn: "dataset_1.table_1",
      type: BigQueryNodeType.VIEW,
      datasetId: "dataset_1",
      viewId: "table_1",
      stateCode: null,
    },
    {
      urn: "dataset_2.table_2",
      type: BigQueryNodeType.VIEW,
      datasetId: "dataset_2",
      viewId: "table_2",
      stateCode: null,
    },
    {
      urn: "dataset_3.table_3",
      type: BigQueryNodeType.VIEW,
      datasetId: "dataset_3",
      viewId: "table_3",
      stateCode: null,
    },
    {
      urn: "dataset_4.table_4",
      type: BigQueryNodeType.VIEW,
      datasetId: "dataset_4",
      viewId: "table_4",
      stateCode: null,
    },
    {
      urn: "dataset_5.table_5",
      type: BigQueryNodeType.VIEW,
      datasetId: "dataset_5",
      viewId: "table_5",
      stateCode: null,
    },
    {
      urn: "dataset_6.table_6",
      type: BigQueryNodeType.VIEW,
      datasetId: "dataset_6",
      viewId: "table_6",
      stateCode: null,
    },
  ],
  references: [
    {
      source: "dataset_1.table_1",
      target: "dataset_3.table_3",
    },
    {
      source: "dataset_2.table_2",
      target: "dataset_3.table_3",
    },
    {
      source: "dataset_3.table_3",
      target: "dataset_4.table_4",
    },
    {
      source: "dataset_2.table_2",
      target: "dataset_5.table_5",
    },
    {
      source: "dataset_1.table_1",
      target: "dataset_4.table_4",
    },
    {
      source: "dataset_3.table_3",
      target: "dataset_5.table_5",
    },
    {
      source: "dataset_4.table_4",
      target: "dataset_6.table_6",
    },
    {
      source: "dataset_5.table_5",
      target: "dataset_6.table_6",
    },
  ],
};
