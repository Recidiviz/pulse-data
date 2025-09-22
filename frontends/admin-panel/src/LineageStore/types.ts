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

/* --- lineage graph types -------------------------------------------------------------
types related to how we represent the how nodes are connected to each other, but does 
not include metadata about how they are displayed
----------------------------------------------------------------------------------------
*/
// a node's unique identifier; for now, for BQ views this is just `dataset.view`
export type NodeUrn = string;

// a node with the most basic information about it -- it's unique identifier and
// if it has dependencies
export type LineageNode = {
  urn: NodeUrn;
  hasUpstream: boolean;
  hasDownstream: boolean;
};

/* --- display graph types -------------------------------------------------------------
types related to how we display the view graph visually using reactflow
----------------------------------------------------------------------------------------
*/
// base type for metadata about what nodes are being displayed in the graph visual
export type GraphDisplayMetadata = LineageNode & {
  urn: NodeUrn;
  isExpandedUpstream?: boolean;
  isExpandedDownstream?: boolean;
  hasUpstream?: boolean;
  hasDownstream?: boolean;
};

// a unique edge id in the form of `e-{sourceUrn}-{targetUrn}` to ensure uniqueness
export type EdgeId = string;

// combines metadata about a node's display and info about the node itself
export type GraphDisplayNode = GraphDisplayMetadata & LineageNode;

// base type that represents an edge connecting two nodes
export interface GraphEdge {
  id: EdgeId;
  source: NodeUrn;
  target: NodeUrn;
}

// which graph direction we are moving in
export enum GraphDirection {
  UPSTREAM = "UPSTREAM",
  DOWNSTREAM = "DOWNSTREAM",
}

// position metadata about where a node is located in the viewport
export type NodePosition = {
  x: number;
  y: number;
};

/* --- graph filter types ----------------------------------------------------------- */

export enum NodeFilterType {
  INCLUDE = "INCLUDE",
  EXCLUDE = "EXCLUDE",
}

export enum NodeFilterKey {
  STATE_CODE_FILTER = "STATE_CODE",
  DATASET_ID_FILTER = "DATASET_ID",
}

/* --- graph api types -------------------------------------------------------------- */

// represents the most basic amount of information we expect the api to return about
// a node.
export type GraphApiNodeType = {
  urn: NodeUrn;
};

// represents a graph "edge", or a reference/dependence between two nodes
export type GraphReferenceType = {
  source: NodeUrn;
  target: NodeUrn;
};

// api rep of the entire graph
export type GraphType = {
  nodes: BigQueryLineageMetadata[];
  references: GraphReferenceType[];
};

/* --- big query nodes info & types ------------------------------------------------- */

export type BigQueryLineageMetadata = {
  urn: NodeUrn;
  type: BigQueryNodeType;
  viewId: string;
  datasetId: string;
  stateCode: string | null;
};

export type BigQueryLineageNode = LineageNode & BigQueryLineageMetadata;

export type BigQueryGraphDisplayNode = GraphDisplayMetadata &
  BigQueryLineageNode;

export enum BigQueryNodeType {
  VIEW = "view",
  SOURCE_TABLE = "source",
}
