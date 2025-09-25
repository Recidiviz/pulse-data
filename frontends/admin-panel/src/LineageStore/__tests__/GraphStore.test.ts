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

import { configure } from "mobx";

import {
  diamondShapedGraph,
  parseGraph,
  xShapedGraph,
} from "../__fixtures__/GraphFixtures";
import { GraphStore } from "../GraphStore";
import { GraphDirection } from "../types";

let store: GraphStore;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
let mockRootStore: any;

beforeEach(() => {
  const mockLineageStore = {
    computeEdgesFromNodes: jest.fn(() => []),
  };
  mockRootStore = {
    lineageStore: mockLineageStore,
    uiStore: {}, // Mock other required properties
  };
  configure({ safeDescriptors: false });
  store = new GraphStore(mockRootStore);
});

describe("contractGraph", () => {
  const parsedXShapeGraph = parseGraph(xShapedGraph);
  const parsedDiamondShapedGraph = parseGraph(diamondShapedGraph);

  //  1     2
  //   \   /
  //     3^
  //   /   \
  //  4     5
  //
  // turns into
  //
  //     3
  //   /   \
  //  4     5
  test("x graph, contract 3 upstream", () => {
    store.nodes = parsedXShapeGraph.nodes;
    store.edges = parsedXShapeGraph.edges;

    store.contractGraph("dataset_3.table_3", GraphDirection.UPSTREAM);
    expect(new Set(store.nodes.map((n) => n.id))).toStrictEqual(
      new Set(["dataset_3.table_3", "dataset_4.table_4", "dataset_5.table_5"])
    );
  });
  //  1     2
  //   \   /
  //     3v
  //   /   \
  //  4     5
  //
  // turns into
  //
  //  1     2
  //   \   /
  //     3
  test("x graph, contract 3 downstream", () => {
    store.nodes = parsedXShapeGraph.nodes;
    store.edges = parsedXShapeGraph.edges;

    store.contractGraph("dataset_3.table_3", GraphDirection.DOWNSTREAM);
    expect(new Set(store.nodes.map((n) => n.id))).toStrictEqual(
      new Set(["dataset_3.table_3", "dataset_1.table_1", "dataset_2.table_2"])
    );
  });
  //  1     2
  //   \   /
  //     3
  //   /   \
  //  4     5^
  //
  // turns into
  //
  //         5
  test("x graph, contract 5 upstream", () => {
    store.nodes = parsedXShapeGraph.nodes;
    store.edges = parsedXShapeGraph.edges;

    store.contractGraph("dataset_5.table_5", GraphDirection.UPSTREAM);
    expect(new Set(store.nodes.map((n) => n.id))).toStrictEqual(
      new Set(["dataset_5.table_5"])
    );
  });
  //  1     2
  //   \   /
  //  |  3  |
  //   /   \
  //  4    5^
  //   \   /
  //     6
  //
  // turns into
  //
  //  4     5
  //   \   /
  //     6
  test("diamond graph, contract 5 upstream", () => {
    store.nodes = parsedDiamondShapedGraph.nodes;
    store.edges = parsedDiamondShapedGraph.edges;

    store.contractGraph("dataset_5.table_5", GraphDirection.UPSTREAM);
    expect(new Set(store.nodes.map((n) => n.id))).toStrictEqual(
      new Set(["dataset_5.table_5", "dataset_4.table_4", "dataset_6.table_6"])
    );
  });
  //  1     2
  //   \   /
  //  |  3^  |
  //   /   \
  //  4     5
  //   \   /
  //     6
  //
  // turns into
  //
  //     3
  //   /   \
  //  4     5
  //   \   /
  //     6
  test("diamond graph, contract 3 upstream", () => {
    store.nodes = parsedDiamondShapedGraph.nodes;
    store.edges = parsedDiamondShapedGraph.edges;

    store.contractGraph("dataset_3.table_3", GraphDirection.UPSTREAM);
    expect(new Set(store.nodes.map((n) => n.id))).toStrictEqual(
      new Set([
        "dataset_5.table_5",
        "dataset_4.table_4",
        "dataset_6.table_6",
        "dataset_3.table_3",
      ])
    );
  });
  //  1     2
  //   \   /
  //  |  3  |
  //   /   \
  //  4     5
  //   \   /
  //     6^
  //
  // turns into
  //
  //    (6)
  test("diamond graph, contract 6 upstream", () => {
    store.nodes = parsedDiamondShapedGraph.nodes;
    store.edges = parsedDiamondShapedGraph.edges;

    store.contractGraph("dataset_6.table_6", GraphDirection.UPSTREAM);
    expect(new Set(store.nodes.map((n) => n.id))).toStrictEqual(
      new Set(["dataset_6.table_6"])
    );
  });
});
