// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2023 Recidiviz, Inc.
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

// --- general constants ---------------------------------------------------------------
export enum DirectIngestInstance {
  PRIMARY = "PRIMARY",
  SECONDARY = "SECONDARY",
}

// --- dataflow-related constants ------------------------------------------------------
export type DataflowJobStatusMetadata = {
  status: DataflowJobState;
  terminationTime: number | undefined;
};

export enum DataflowJobState {
  SUCCEEDED = "SUCCEEDED",
  FAILED = "FAILED",
  NO_JOB_RUNS = "NO_JOB_RUNS",
}

export type DataflowIngestPipelineStatus = {
  id: string;
  projectId: string;
  name: string;
  createTime: number;
  startTime: number;
  terminationTime: number;
  terminationState: string;
  location: string;
  duration: number;
};

export type DataflowIngestPipelineJobResponse = {
  [stateCode: string]: DataflowIngestPipelineStatus | null;
};

export type DataflowIngestPipelineAdditionalMetadata = {
  ingestViewResultsDatasetName: string;
  stateResultsDatasetName: string;
};

export interface DataflowIngestRawDataWatermarks {
  [fileName: string]: Date;
}

// --- ingest-related constants --------------------------------------------------------
export interface IngestViewResultRowCounts {
  [ingestViewName: string]: number;
}

export interface StateDatasetRowCounts {
  [dataset: string]: number;
}

// --- raw data import dag summary related constants -----------------------------------

export enum RawDataImportRunState {
  SUCCEEDED = "SUCCEEDED",
  FAILED = "FAILED",
  IN_PROGRESS = "IN PROGRESS",
}

export type RawDataImportRunStatus = {
  importStatus: RawDataImportRunState;
  fileCount: number;
};

export type RawDataImportRunStatusInfo = {
  importRunStart: string | undefined;
  countByStatusBucket: RawDataImportRunStatus[];
};

export type RawDataImportRunStatusResponse = {
  [stateCode: string]: RawDataImportRunStatusInfo;
};

// --- raw data file tag detail related constants --------------------------------------

export enum RawDataImportStatus {
  STARTED = "STARTED",
  SUCCEEDED = "SUCCEEDED",
  DEFERRED = "DEFERRED",
  FAILED_UNKNOWN = "FAILED_UNKNOWN",
  FAILED_LOAD_STEP = "FAILED_LOAD_STEP",
  FAILED_PRE_IMPORT_NORMALIZATION_STEP = "FAILED_PRE_IMPORT_NORMALIZATION_STEP",
  FAILED_VALIDATION_STEP = "FAILED_VALIDATION_STEP",
  FAILED_IMPORT_BLOCKED = "FAILED_IMPORT_BLOCKED",
}

export type RawDataFileTagImport = {
  importRunId: number;
  fileId: number;
  dagRunId: string;
  updateDatetime: string;
  importRunStart: string;
  importStatus: RawDataImportStatus;
  importStatusDescription: string;
  historicalDiffsActive: boolean;
  rawRowCount: number;
  isInvalidated: boolean;
  netNewOrUpdatedRows: number | null;
  deletedRows: number | null;
};

export enum RawDataPruningStatus {
  AUTOMATIC = "AUTOMATIC",
  // TODO(#51884) Remove once manual pruning is fully deprecated
  MANUAL = "MANUAL",
  NOT_PRUNED = "NOT_PRUNED",
}

export type RawFileConfigSummary = {
  fileTag: string;
  fileDescription: string;
  updateCadence: string;
  encoding: string;
  separator: string;
  lineTerminator: string;
  exportLookbackWindow: string;
  isCodeFile: boolean;
  isChunkedFile: boolean;
  pruningStatus: RawDataPruningStatus;
  inferColumns: boolean;
};

// --- raw data resource lock related constants ----------------------------------------

export enum RawDataResourceLockResource {
  BUCKET = "BUCKET",
  OPERATIONS_DATABASE = "OPERATIONS_DATABASE",
  BIG_QUERY_RAW_DATA_DATASET = "BIG_QUERY_RAW_DATA_DATASET",
}

export enum RawDataResourceLockActor {
  ADHOC = "ADHOC",
  PROCESS = "PROCESS",
}

export enum ResourceLockState {
  ADHOC_HELD = "ADHOC_HELD",
  PROCESS_HELD = "PROCESS_HELD",
  FREE = "FREE",
  MIXED = "MIXED",
  UNKNOWN = "UNKNOWN",
}

export const resourceLockHeldStates = new Set<ResourceLockState>([
  ResourceLockState.ADHOC_HELD,
  ResourceLockState.PROCESS_HELD,
  ResourceLockState.MIXED,
]);

export type ResourceLockMetadata = {
  actors: ResourceLockActorDescription;
  resources: ResourceLockResourceDescription;
};

export type ResourceLockResourceDescription = {
  [resource in RawDataResourceLockResource]: string;
};

export type ResourceLockActorDescription = {
  [actor in RawDataResourceLockActor]: string;
};

export type ResourceLockStatus = {
  lockId: number;
  rawDataInstance: DirectIngestInstance;
  lockAcquisitionTime: string;
  ttlSeconds: number;
  description: string;
  resource: RawDataResourceLockResource;
  released: boolean;
  actor: RawDataResourceLockActor;
};

export type RawDataResourceLockStatuses = {
  [lockStatus in keyof RawDataResourceLockResource]:
    | RawDataResourceLockActor
    | undefined;
};

export type RawDataResourceLockStatusesResponse = {
  [stateCode: string]: RawDataResourceLockStatuses;
};

// --- raw data related constants ------------------------------------------------------

export type IngestInstanceResources = {
  storageDirectoryPath: string;
  ingestBucketPath: string;
};

export type IngestRawFileProcessingStatus = {
  fileTag: string;
  hasConfig: boolean;
  numberFilesInBucket: number;
  numberUnprocessedFiles: number;
  numberUngroupedFiles: number;
  numberProcessedFiles: number;
  latestDiscoveryTime: string | null;
  latestProcessedTime: string | null;
  latestUpdateDatetime: string | null;
  isStale: boolean;
};

// --- string constants ----------------------------------------------------------------

export const ANCHOR_DATAFLOW_LATEST_JOB = "dataflow_latest_job";
export const ANCHOR_INGEST_RAW_DATA = "ingest_raw_data";
export const ANCHOR_INGEST_RESOURCES = "ingest_resources";

export const FILE_TAG_UNNORMALIZED = "UNNORMALIZED"; // special tag for files that don't follow normalized format
export const FILE_TAG_IGNORED_IN_SUBDIRECTORY = "IGNORED_IN_SUBDIRECTORY"; // special tag for files that are not in the base directory

export const SPECIAL_FILE_TAGS = [
  FILE_TAG_UNNORMALIZED,
  FILE_TAG_IGNORED_IN_SUBDIRECTORY,
];
