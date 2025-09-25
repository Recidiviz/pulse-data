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
  lightlyConnectedSubGraphs,
  parseGraph,
  xShapedGraph,
} from "../__fixtures__/GraphHelpersFixture";
import {
  findLargestConnectedGraphOfPossibleUnconnectedGraphs,
  findUnreachableNodes,
} from "../GraphHelpers";

// -------------------------------------------------------------------------------------
// ----  findUnreachableNode tests -----------------------------------------------------
// -------------------------------------------------------------------------------------
describe("findUnreachableNode", () => {
  const parsedXShapeGraph = parseGraph(xShapedGraph);
  const parsedDiamondShapedGraph = parseGraph(diamondShapedGraph);

  test("x graph, middle node, no filters", () => {
    const { connectedNotFilteredUrns, unreachableUrns } = findUnreachableNodes(
      "dataset_3.table_3",
      new Set(parsedXShapeGraph.nodes.map((n) => n.id)),
      parsedXShapeGraph.urnToUpstreamUrns,
      parsedXShapeGraph.urnToDownstreamUrns,
      new Set()
    );
    expect(unreachableUrns).toStrictEqual(new Set());
    expect(connectedNotFilteredUrns).toStrictEqual(
      new Set([
        "dataset_1.table_1",
        "dataset_2.table_2",
        "dataset_3.table_3",
        "dataset_4.table_4",
        "dataset_5.table_5",
      ])
    );
  });
  test("x graph, middle node, one filter", () => {
    const { connectedNotFilteredUrns, unreachableUrns } = findUnreachableNodes(
      "dataset_3.table_3",
      new Set(parsedXShapeGraph.nodes.map((n) => n.id)),
      parsedXShapeGraph.urnToUpstreamUrns,
      parsedXShapeGraph.urnToDownstreamUrns,
      new Set(parsedXShapeGraph.nodes.slice(0, 1).map((n) => n.id)) // filter out id 1
    );
    expect(unreachableUrns).toStrictEqual(new Set());
    expect(connectedNotFilteredUrns).toStrictEqual(
      new Set([
        "dataset_2.table_2",
        "dataset_3.table_3",
        "dataset_4.table_4",
        "dataset_5.table_5",
      ])
    );
  });
  test("x graph, node 1, no filter, all reachable", () => {
    const { connectedNotFilteredUrns, unreachableUrns } = findUnreachableNodes(
      "dataset_1.table_1",
      new Set(parsedXShapeGraph.nodes.map((n) => n.id)),
      parsedXShapeGraph.urnToUpstreamUrns,
      parsedXShapeGraph.urnToDownstreamUrns,
      new Set()
    );
    expect(unreachableUrns).toStrictEqual(new Set());
    expect(connectedNotFilteredUrns).toStrictEqual(
      new Set([
        "dataset_1.table_1",
        "dataset_2.table_2",
        "dataset_3.table_3",
        "dataset_4.table_4",
        "dataset_5.table_5",
      ])
    );
  });
  test("x graph, node 1, filter out 3, blocking 2 and 4 and 5", () => {
    const { connectedNotFilteredUrns, unreachableUrns } = findUnreachableNodes(
      "dataset_1.table_1",
      new Set(parsedXShapeGraph.nodes.map((n) => n.id)),
      parsedXShapeGraph.urnToUpstreamUrns,
      parsedXShapeGraph.urnToDownstreamUrns,
      new Set(parsedXShapeGraph.nodes.slice(2, 3).map((n) => n.id)) // filter out id 3
    );
    expect(unreachableUrns).toStrictEqual(
      new Set(["dataset_2.table_2", "dataset_4.table_4", "dataset_5.table_5"])
    );
    expect(connectedNotFilteredUrns).toStrictEqual(
      new Set(["dataset_1.table_1"])
    );
  });
  test("diamond graph, node 1, no filters", () => {
    const { connectedNotFilteredUrns, unreachableUrns } = findUnreachableNodes(
      "dataset_1.table_1",
      new Set(parsedDiamondShapedGraph.nodes.map((n) => n.id)),
      parsedDiamondShapedGraph.urnToUpstreamUrns,
      parsedDiamondShapedGraph.urnToDownstreamUrns,
      new Set()
    );
    expect(unreachableUrns).toStrictEqual(new Set());
    expect(connectedNotFilteredUrns).toStrictEqual(
      new Set([
        "dataset_1.table_1",
        "dataset_2.table_2",
        "dataset_3.table_3",
        "dataset_4.table_4",
        "dataset_5.table_5",
        "dataset_6.table_6",
      ])
    );
  });
  test("diamond graph, node 1, filter 3, all reachable", () => {
    const { connectedNotFilteredUrns, unreachableUrns } = findUnreachableNodes(
      "dataset_1.table_1",
      new Set(parsedDiamondShapedGraph.nodes.map((n) => n.id)),
      parsedDiamondShapedGraph.urnToUpstreamUrns,
      parsedDiamondShapedGraph.urnToDownstreamUrns,
      new Set(parsedDiamondShapedGraph.nodes.slice(2, 3).map((n) => n.id)) // filter out id 3
    );
    expect(unreachableUrns).toStrictEqual(new Set());
    expect(connectedNotFilteredUrns).toStrictEqual(
      new Set([
        "dataset_1.table_1",
        "dataset_2.table_2",
        "dataset_4.table_4",
        "dataset_5.table_5",
        "dataset_6.table_6",
      ])
    );
  });
  test("diamond graph, node 1, filter 3 and 5, cant reach 2", () => {
    const { connectedNotFilteredUrns, unreachableUrns } = findUnreachableNodes(
      "dataset_1.table_1",
      new Set(parsedDiamondShapedGraph.nodes.map((n) => n.id)),
      parsedDiamondShapedGraph.urnToUpstreamUrns,
      parsedDiamondShapedGraph.urnToDownstreamUrns,
      new Set(
        [
          ...parsedDiamondShapedGraph.nodes.slice(2, 3), // filter out id 3
          ...parsedDiamondShapedGraph.nodes.slice(4, 5), // filter out id 5
        ].map((n) => n.id)
      )
    );
    expect(unreachableUrns).toStrictEqual(new Set(["dataset_2.table_2"]));
    expect(connectedNotFilteredUrns).toStrictEqual(
      new Set(["dataset_1.table_1", "dataset_4.table_4", "dataset_6.table_6"])
    );
  });
  test("diamond graph, node 3, filter 5, all reachable", () => {
    const { connectedNotFilteredUrns, unreachableUrns } = findUnreachableNodes(
      "dataset_3.table_3",
      new Set(parsedDiamondShapedGraph.nodes.map((n) => n.id)),
      parsedDiamondShapedGraph.urnToUpstreamUrns,
      parsedDiamondShapedGraph.urnToDownstreamUrns,
      new Set(parsedDiamondShapedGraph.nodes.slice(4, 5).map((n) => n.id)) // filter out id 5
    );
    expect(unreachableUrns).toStrictEqual(new Set());
    expect(connectedNotFilteredUrns).toStrictEqual(
      new Set([
        "dataset_1.table_1",
        "dataset_2.table_2",
        "dataset_3.table_3",
        "dataset_4.table_4",
        "dataset_6.table_6",
      ])
    );
  });
  test("diamond graph, node 3, filter 4 and 5, cant reach 6", () => {
    const { connectedNotFilteredUrns, unreachableUrns } = findUnreachableNodes(
      "dataset_3.table_3",
      new Set(parsedDiamondShapedGraph.nodes.map((n) => n.id)),
      parsedDiamondShapedGraph.urnToUpstreamUrns,
      parsedDiamondShapedGraph.urnToDownstreamUrns,
      new Set(parsedDiamondShapedGraph.nodes.slice(3, 5).map((n) => n.id)) // filter out ids 4 and 5
    );
    expect(unreachableUrns).toStrictEqual(new Set(["dataset_6.table_6"]));
    expect(connectedNotFilteredUrns).toStrictEqual(
      new Set(["dataset_1.table_1", "dataset_2.table_2", "dataset_3.table_3"])
    );
  });
});

// -------------------------------------------------------------------------------------
// ----  findLargestConnectedGraphOfPossibleUnconnectedGraphs tests --------------------
// -------------------------------------------------------------------------------------

describe("findLargestConnectedGraphOfPossibleUnconnectedGraphs", () => {
  const parsedXShapeGraph = parseGraph(xShapedGraph);
  const parsedDiamondShapedGraph = parseGraph(diamondShapedGraph);
  const parsedLightlyConnectedSubGraphs = parseGraph(lightlyConnectedSubGraphs);

  test("x graph, no filters", () => {
    const largestConnectedGraph =
      findLargestConnectedGraphOfPossibleUnconnectedGraphs(
        parsedXShapeGraph.nodes,
        parsedXShapeGraph.edges,
        []
      );
    expect(largestConnectedGraph).toStrictEqual(
      new Set([
        "dataset_1.table_1",
        "dataset_2.table_2",
        "dataset_3.table_3",
        "dataset_4.table_4",
        "dataset_5.table_5",
      ])
    );
  });
  test("x graph, filter 1", () => {
    const largestConnectedGraph =
      findLargestConnectedGraphOfPossibleUnconnectedGraphs(
        parsedXShapeGraph.nodes,
        parsedXShapeGraph.edges,
        parsedXShapeGraph.nodes.slice(0, 1) // filter 1
      );
    expect(largestConnectedGraph).toStrictEqual(
      new Set([
        "dataset_2.table_2",
        "dataset_3.table_3",
        "dataset_4.table_4",
        "dataset_5.table_5",
      ])
    );
  });
  test("x graph, filter 1 and 2", () => {
    const largestConnectedGraph =
      findLargestConnectedGraphOfPossibleUnconnectedGraphs(
        parsedXShapeGraph.nodes,
        parsedXShapeGraph.edges,
        parsedXShapeGraph.nodes.slice(0, 2) // filter 1 and 2
      );
    expect(largestConnectedGraph).toStrictEqual(
      new Set(["dataset_3.table_3", "dataset_4.table_4", "dataset_5.table_5"])
    );
  });
  test("x graph, filter 3", () => {
    const largestConnectedGraph =
      findLargestConnectedGraphOfPossibleUnconnectedGraphs(
        parsedXShapeGraph.nodes,
        parsedXShapeGraph.edges,
        parsedXShapeGraph.nodes.slice(2, 3) // filter 3
      );
    // filtering out 3 means all nodes are unconnected so we should just have four isolated
    // single nodes
    expect(largestConnectedGraph.size).toBe(1);
    // any node is technically correct and we are not pulling from the nodes deterministically
    expect(Array.from(largestConnectedGraph)[0]).not.toBe("dataset_3.table_3");
  });
  test("x graph, filter all", () => {
    const largestConnectedGraph =
      findLargestConnectedGraphOfPossibleUnconnectedGraphs(
        parsedXShapeGraph.nodes,
        parsedXShapeGraph.edges,
        parsedXShapeGraph.nodes
      );
    // filtering out 3 means all nodes are unconnected so we should just have four isolated
    // single nodes
    expect(largestConnectedGraph).toStrictEqual(new Set());
  });
  test("diamond graph, no filters", () => {
    const largestConnectedGraph =
      findLargestConnectedGraphOfPossibleUnconnectedGraphs(
        parsedDiamondShapedGraph.nodes,
        parsedDiamondShapedGraph.edges,
        []
      );
    expect(largestConnectedGraph).toStrictEqual(
      new Set([
        "dataset_1.table_1",
        "dataset_2.table_2",
        "dataset_3.table_3",
        "dataset_4.table_4",
        "dataset_5.table_5",
        "dataset_6.table_6",
      ])
    );
  });
  test("diamond graph, filter 6, no-op", () => {
    const largestConnectedGraph =
      findLargestConnectedGraphOfPossibleUnconnectedGraphs(
        parsedDiamondShapedGraph.nodes,
        parsedDiamondShapedGraph.edges,
        parsedDiamondShapedGraph.nodes.slice(5, 6) // filter out 6
      );
    expect(largestConnectedGraph).toStrictEqual(
      new Set([
        "dataset_1.table_1",
        "dataset_2.table_2",
        "dataset_3.table_3",
        "dataset_4.table_4",
        "dataset_5.table_5",
      ])
    );
  });
  test("diamond graph, filter 4, no-op", () => {
    const largestConnectedGraph =
      findLargestConnectedGraphOfPossibleUnconnectedGraphs(
        parsedDiamondShapedGraph.nodes,
        parsedDiamondShapedGraph.edges,
        parsedDiamondShapedGraph.nodes.slice(3, 4) // filter out 4
      );
    expect(largestConnectedGraph).toStrictEqual(
      new Set([
        "dataset_1.table_1",
        "dataset_2.table_2",
        "dataset_3.table_3",
        "dataset_5.table_5",
        "dataset_6.table_6",
      ])
    );
  });
  test("diamond graph, filter 3 and 4, makes 2 subgraphs, of which 2-5-6 is biggest", () => {
    const largestConnectedGraph =
      findLargestConnectedGraphOfPossibleUnconnectedGraphs(
        parsedDiamondShapedGraph.nodes,
        parsedDiamondShapedGraph.edges,
        parsedDiamondShapedGraph.nodes.slice(2, 4) // filter out 3, 4
      );
    expect(largestConnectedGraph).toStrictEqual(
      new Set(["dataset_5.table_5", "dataset_6.table_6", "dataset_2.table_2"])
    );
  });
  test("diamond graph, filter 3 and 4, makes 2 subgraphs, of which 2-5-6 is biggest", () => {
    const largestConnectedGraph =
      findLargestConnectedGraphOfPossibleUnconnectedGraphs(
        parsedDiamondShapedGraph.nodes,
        parsedDiamondShapedGraph.edges,
        parsedDiamondShapedGraph.nodes.slice(2, 4) // filter out 3, 4
      );
    expect(largestConnectedGraph).toStrictEqual(
      new Set(["dataset_5.table_5", "dataset_6.table_6", "dataset_2.table_2"])
    );
  });
  test("lightly connected graph, no filters", () => {
    const largestConnectedGraph =
      findLargestConnectedGraphOfPossibleUnconnectedGraphs(
        parsedLightlyConnectedSubGraphs.nodes,
        parsedLightlyConnectedSubGraphs.edges,
        []
      );
    expect(largestConnectedGraph).toStrictEqual(
      new Set([
        "dataset_1.table_1",
        "dataset_2.table_2",
        "dataset_3.table_3",
        "dataset_4.table_4",
        "dataset_5.table_5",
        "dataset_6.table_6",
        "dataset_7.table_7",
        "dataset_8.table_8",
        "dataset_9.table_9",
        "dataset_10.table_10",
      ])
    );
  });
  test("lightly connected graph, filter 2, 4 and 10 which creates 3 unconnected graphs of sizes 2 2 and 3", () => {
    const largestConnectedGraph =
      findLargestConnectedGraphOfPossibleUnconnectedGraphs(
        parsedLightlyConnectedSubGraphs.nodes,
        parsedLightlyConnectedSubGraphs.edges,
        [
          parsedLightlyConnectedSubGraphs.nodes[1],
          parsedLightlyConnectedSubGraphs.nodes[3],
          parsedLightlyConnectedSubGraphs.nodes[9],
        ]
      );
    expect(largestConnectedGraph).toStrictEqual(
      new Set(["dataset_7.table_7", "dataset_8.table_8", "dataset_9.table_9"])
    );
  });
  test("lightly connected graph, filter 10 which creates 3 unconnected graphs of size 3", () => {
    const largestConnectedGraph =
      findLargestConnectedGraphOfPossibleUnconnectedGraphs(
        parsedLightlyConnectedSubGraphs.nodes,
        parsedLightlyConnectedSubGraphs.edges,
        [
          parsedLightlyConnectedSubGraphs.nodes[1],
          parsedLightlyConnectedSubGraphs.nodes[3],
          parsedLightlyConnectedSubGraphs.nodes[9],
        ]
      );
    expect(largestConnectedGraph.size).toBe(3);
    expect(largestConnectedGraph).not.toContain("dataset_10.table_10");
  });
});
