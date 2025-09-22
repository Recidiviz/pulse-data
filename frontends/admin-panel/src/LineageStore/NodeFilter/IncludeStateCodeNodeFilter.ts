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

export class IncludeStateCodeNodeFilter implements NodeFilter {
  key: NodeFilterKey = NodeFilterKey.STATE_CODE_FILTER;

  type: NodeFilterType = NodeFilterType.INCLUDE;

  // eslint-disable-next-line no-useless-constructor
  constructor(public value: string) {}

  shouldIncludeNode(node: Node<BigQueryGraphDisplayNode>): boolean {
    // allow state-agnostic and matching state-specific nodes through
    return node.data.stateCode === null || node.data.stateCode === this.value;
  }
}
