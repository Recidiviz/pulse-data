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

import { Tooltip } from "antd";
import { BaseOptionType, DefaultOptionType } from "antd/lib/select";

import { HydrationState } from "../InsightsStore/types";
import { NodeFilter } from "./NodeFilter/NodeFilter";
import { BigQueryGraphDisplayNode } from "./types";

/**
 * Throws a new error, using |errorMessage|.
 */
export function throwExpression(errorMessage: string): never {
  throw new Error(errorMessage);
}

/**
 * Gets the Error.message value if |error| is an Error; otherwise
 * stringifies the |error|.
 */
export function getErrorMessage(error: unknown): string {
  if (error instanceof Error) return error.message;
  return String(error);
}

export function buildSelectOptionForFilter(filter: NodeFilter): BaseOptionType {
  return { value: filter.value, filter };
}

function createBigQueryNodeLabel(viewId: string, urn: string) {
  return (
    <Tooltip title={urn}>
      <span>{viewId}</span>
    </Tooltip>
  );
}

export function createBigQueryNodeAutoCompleteGroups(
  nodes: BigQueryGraphDisplayNode[]
): DefaultOptionType[] {
  const datasetToNode = Object.groupBy(nodes, (node) => node.datasetId);
  const newOptions = Object.keys(datasetToNode).map((datasetId: string) => ({
    label: datasetId,
    options: datasetToNode[datasetId]?.map((node) => ({
      label: createBigQueryNodeLabel(node.viewId, node.urn),
      value: node.urn,
    })),
  }));
  return newOptions;
}

/**
 * Returns true if state has reached a successful terminal state and false otherwise.
 */
export function isHydrated(state: HydrationState): boolean {
  switch (state.status) {
    case "hydrated":
      return true;
    case "failed":
    case "loading":
    case "needs hydration":
      return false;
    default: {
      const exhaustiveCheck: never = state;
      throwExpression(`Unknown hydration state: ${exhaustiveCheck}`);
    }
  }
}
