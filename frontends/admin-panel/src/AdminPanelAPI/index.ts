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

import MetadataDataset from "../models/MetadataDatasets";
import { postWithURLAndBody } from "./utils";

// Fetch dataset metadata
export const fetchColumnObjectCountsByValue = async (
  metadataDataset: MetadataDataset,
  table: string,
  column: string
): Promise<Response> => {
  return postWithURLAndBody(
    `/api/${metadataDataset}/fetch_column_object_counts_by_value`,
    { table, column }
  );
};

export const fetchTableNonNullCountsByColumn = async (
  metadataDataset: MetadataDataset,
  table: string
): Promise<Response> => {
  return postWithURLAndBody(
    `/api/${metadataDataset}/fetch_table_nonnull_counts_by_column`,
    { table }
  );
};

export const fetchObjectCountsByTable = async (
  metadataDataset: MetadataDataset
): Promise<Response> => {
  return postWithURLAndBody(
    `/api/${metadataDataset}/fetch_object_counts_by_table`
  );
};

// Fetch data freshness
export const fetchDataFreshness = async (): Promise<Response> => {
  return postWithURLAndBody("/api/ingest_metadata/data_freshness");
};

// Fetch validation status
export const fetchValidationStateCodes = async (): Promise<Response> => {
  return postWithURLAndBody("/api/validation_metadata/state_codes");
};

export const fetchValidationStatus = async (): Promise<Response> => {
  return postWithURLAndBody("/api/validation_metadata/status");
};

export const fetchValidationDetails = async (
  validationName: string,
  stateCode: string
): Promise<Response> => {
  return postWithURLAndBody(
    `/api/validation_metadata/status/${validationName}/${stateCode}`
  );
};

export const fetchValidationDescription = async (
  validationName: string
): Promise<Response> => {
  return postWithURLAndBody(
    `/api/validation_metadata/description/${validationName}`
  );
};

export const fetchValidationErrorTable = async (
  validationName: string,
  stateCode: string
): Promise<Response> => {
  return postWithURLAndBody(
    `/api/validation_metadata/error_table/${validationName}/${stateCode}`
  );
};

export {
  acquireBQExportLock,
  deleteDatabaseImportGCSFiles,
  exportDatabaseToGCS,
  fetchIngestStateCodes,
  getIngestInstanceSummaries,
  getIngestQueuesState,
  importDatabaseFromGCS,
  pauseDirectIngestInstance,
  releaseBQExportLock,
  startIngestRun,
  unpauseDirectIngestInstance,
  updateIngestQueuesState,
} from "./IngestOperations";
export {
  fetchEmailStateCodes,
  fetchETLViewIds,
  fetchRosterStateCodes,
  fetchRawFilesStateCodes,
  generateNonETLExports,
  getPOFeedback,
  runCloudSQLImport,
} from "./LineStaffTools";
export { getAgencies } from "./JusticeCountsTools";
