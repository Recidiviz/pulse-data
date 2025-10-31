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

import {
  BigQueryGraphDisplayNode,
  NodeFilterKey,
  NodeFilterType,
} from "../types";
import { NodeFilter } from "./NodeFilter";

export class ExcludeDatasetPrefixNodeFilter implements NodeFilter {
  key: NodeFilterKey = NodeFilterKey.DATASET_ID_STARTS_WITH_FILTER;

  type: NodeFilterType = NodeFilterType.EXCLUDE;

  // eslint-disable-next-line no-useless-constructor
  constructor(public value: string) {}

  shouldIncludeNode(node: Node<BigQueryGraphDisplayNode>): boolean {
    return node.data.datasetId.startsWith(this.value);
  }
}
