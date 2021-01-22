// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2020 Recidiviz, Inc.
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
export const STATE_METADATA_ROUTE = "/admin/ingest_metadata/state";
export const DB_ROUTE_TEMPLATE = "/admin/ingest_metadata/state/:table";
export const COLUMN_ROUTE_TEMPLATE =
  "/admin/ingest_metadata/state/:table/:column";

export const routeForTable = (table: string): string => {
  return `${STATE_METADATA_ROUTE}/${table}`;
};

export const routeForColumn = (table: string, column: string): string => {
  return `${STATE_METADATA_ROUTE}/${table}/${column}`;
};

export const DATA_FRESHNESS_ROUTE = "/admin/ingest_metadata/data_freshness";
