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
import { ExcludeDatasetNodeFilter } from "./ExcludeDatasetNodeFilter";
import { IncludeStateCodeNodeFilter } from "./IncludeStateCodeNodeFilter";
import { NodeFilter } from "./NodeFilter";

export function buildNodeFilter(
  type: NodeFilterType,
  key: NodeFilterKey,
  value: string
): NodeFilter {
  if (
    type === NodeFilterType.EXCLUDE &&
    key === NodeFilterKey.DATASET_ID_FILTER
  ) {
    return new ExcludeDatasetNodeFilter(value);
  }
  if (
    type === NodeFilterType.INCLUDE &&
    key === NodeFilterKey.STATE_CODE_FILTER
  ) {
    return new IncludeStateCodeNodeFilter(value);
  }
  throw new Error(`Unknown node filter type [${type}] and key [${key}] combo`);
}

export function applyFiltersToNode(
  node: Node<BigQueryGraphDisplayNode>,
  includeFilters: NodeFilter[],
  excludeFilters: NodeFilter[]
): boolean {
  if (node.hidden === true) {
    return false;
  }

  // apply all "include" filters -- is a composite OR
  const includeFiltersResult = includeFilters.reduce(
    (shouldFilterOut, filter) =>
      shouldFilterOut || filter.shouldIncludeNode(node),
    includeFilters.length === 0
  );
  // apply all "exclude" filters -- is a composite AND NOT
  const excludeFiltersResult = excludeFilters.reduce(
    (shouldFilterOut, filter) =>
      shouldFilterOut && !filter.shouldIncludeNode(node),
    true
  );

  return includeFiltersResult && excludeFiltersResult;
}
