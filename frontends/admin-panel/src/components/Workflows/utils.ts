// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2024 Recidiviz, Inc.
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

import { ColumnsType, ColumnType } from "antd/lib/table";

import { WORKFLOWS_OPPORTUNITIES_ROUTE } from "../../navigation/LineStaffTools";

export function buildColumns<T>(
  specs: Partial<Record<keyof T, Partial<ColumnType<T>>>>
): ColumnsType<T> {
  return Object.entries(specs).map(([k, col]) => ({
    title: k,
    dataIndex: k,
    key: k,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    sorter: (a: any, b: any) =>
      a[k]?.localeCompare ? a[k].localeCompare(b[k]) : a[k] - b[k],
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    onFilter: (value, record: any) => record[k] === value,
    ...(col as Partial<ColumnType<T>>), // I'm not sure why this isn't inferred
  }));
}

export function buildRoute(
  stateCode: string,
  opportunityType?: string,
  configId?: string | number
) {
  let route = WORKFLOWS_OPPORTUNITIES_ROUTE.replace(":stateCode", stateCode);
  if (opportunityType) route += `/${opportunityType}`;
  if (configId) route += `/configurations/${configId}`;
  return route;
}
