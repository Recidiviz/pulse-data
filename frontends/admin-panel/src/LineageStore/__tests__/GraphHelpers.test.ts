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

import {
  diamondShapedGraph,
  parseGraph,
  xShapedGraph,
} from "../__fixtures__/GraphHelpersFixture";
import { findUnreachableNodes } from "../GraphHelpers";

describe("findUnreachableNode", () => {
  const parsedXShapeGraph = parseGraph(xShapedGraph);
  const parsedDiamondShapedGraph = parseGraph(diamondShapedGraph);

  test("x graph, middle node, no filters", () => {
    expect(
      findUnreachableNodes(
        "dataset_3.table_3",
        parsedXShapeGraph.nodes,
        parsedXShapeGraph.edges,
        []
      )
    ).toStrictEqual(new Set());
  });
  test("x graph, middle node, one filter", () => {
    expect(
      findUnreachableNodes(
        "dataset_3.table_3",
        parsedXShapeGraph.nodes,
        parsedXShapeGraph.edges,
        parsedXShapeGraph.nodes.slice(0, 1) // filter out id 1
      )
    ).toStrictEqual(new Set());
  });
  test("x graph, node 1, no filter, all reachable", () => {
    expect(
      findUnreachableNodes(
        "dataset_1.table_1",
        parsedXShapeGraph.nodes,
        parsedXShapeGraph.edges,
        []
      )
    ).toStrictEqual(new Set());
  });
  test("x graph, node 1, filter out 3, blocking 2 and 4 and 5", () => {
    expect(
      findUnreachableNodes(
        "dataset_1.table_1",
        parsedXShapeGraph.nodes,
        parsedXShapeGraph.edges,
        parsedXShapeGraph.nodes.slice(2, 3) // filter out id 2
      )
    ).toStrictEqual(
      new Set(["dataset_2.table_2", "dataset_4.table_4", "dataset_5.table_5"])
    );
  });
  test("diamond graph, node 1, no filters", () => {
    expect(
      findUnreachableNodes(
        "dataset_1.table_1",
        parsedDiamondShapedGraph.nodes,
        parsedDiamondShapedGraph.edges,
        []
      )
    ).toStrictEqual(new Set());
  });
  test("diamond graph, node 1, filter 3, all reachable", () => {
    expect(
      findUnreachableNodes(
        "dataset_1.table_1",
        parsedDiamondShapedGraph.nodes,
        parsedDiamondShapedGraph.edges,
        parsedDiamondShapedGraph.nodes.slice(2, 3) // filter out id 2
      )
    ).toStrictEqual(new Set());
  });
  test("diamond graph, node 1, filter 3 and 5, cant reach 2", () => {
    expect(
      findUnreachableNodes(
        "dataset_1.table_1",
        parsedDiamondShapedGraph.nodes,
        parsedDiamondShapedGraph.edges,
        [
          ...parsedDiamondShapedGraph.nodes.slice(2, 3), // filter out id 2
          ...parsedDiamondShapedGraph.nodes.slice(4, 5), // filter out id 4
        ]
      )
    ).toStrictEqual(new Set(["dataset_2.table_2"]));
  });
  test("diamond graph, node 3, filter 5, all reachable", () => {
    expect(
      findUnreachableNodes(
        "dataset_3.table_3",
        parsedDiamondShapedGraph.nodes,
        parsedDiamondShapedGraph.edges,
        parsedDiamondShapedGraph.nodes.slice(4, 5) // filter out id 4
      )
    ).toStrictEqual(new Set());
  });
  test("diamond graph, node 3, filter 4 and 5, cant reach 6", () => {
    expect(
      findUnreachableNodes(
        "dataset_3.table_3",
        parsedDiamondShapedGraph.nodes,
        parsedDiamondShapedGraph.edges,
        parsedDiamondShapedGraph.nodes.slice(3, 5) // filter out ids 3 and 4
      )
    ).toStrictEqual(new Set(["dataset_6.table_6"]));
  });
});
