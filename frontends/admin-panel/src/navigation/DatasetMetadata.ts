// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2021 Recidiviz, Inc.
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

import MetadataDataset from "../models/MetadataDatasets";

export const VALIDATION_STATUS_ROUTE = `/admin/${MetadataDataset.VALIDATION}/status`;
export const VALIDATION_STATUS_FAILURE_SUMMARY_ROUTE = `${VALIDATION_STATUS_ROUTE}/failure_summary`;
export const VALIDATION_STATUS_FULL_RESULTS_ROUTE = `${VALIDATION_STATUS_ROUTE}/full_results`;
export const VALIDATION_DETAIL_ROUTE_TEMPLATE = `${VALIDATION_STATUS_ROUTE}/details/:validationName`;

export const routeForValidationDetail = (validationName?: string): string => {
  if (validationName === undefined) {
    return VALIDATION_STATUS_FAILURE_SUMMARY_ROUTE;
  }
  return VALIDATION_DETAIL_ROUTE_TEMPLATE.replace(
    ":validationName",
    validationName
  );
};

export const METADATA_DATASET_ROUTE_TEMPLATE = "/admin/:dataset/dataset";
export const METADATA_TABLE_ROUTE_TEMPLATE = "/admin/:dataset/dataset/:table";
export const METADATA_COLUMN_ROUTE_TEMPLATE =
  "/admin/:dataset/dataset/:table/:column";

export const routeForMetadataDataset = (
  metadataDataset: MetadataDataset
): string => {
  return `/admin/${metadataDataset}/dataset`;
};

export const routeForMetadataTable = (
  metadataDataset: MetadataDataset,
  table: string
): string => {
  return `${routeForMetadataDataset(metadataDataset)}/${table}`;
};

export const routeForMetadataColumn = (
  metadataDataset: MetadataDataset,
  table: string,
  column: string
): string => {
  return `${routeForMetadataDataset(metadataDataset)}/${table}/${column}`;
};

export const getBreadCrumbLabel = (
  metadataDataset: MetadataDataset
): string => {
  if (metadataDataset === MetadataDataset.INGEST) {
    return "State";
  }
  if (metadataDataset === MetadataDataset.VALIDATION) {
    return "Validation";
  }
  return "";
};

export function addStateCodeQueryToLink(
  link: string,
  stateCode: string | undefined | null
): string {
  return stateCode ? `${link}?stateCode=${stateCode}` : link;
}
