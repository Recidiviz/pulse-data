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

export enum DirectIngestInstance {
  PRIMARY = "PRIMARY",
  SECONDARY = "SECONDARY",
}

export enum JobState {
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

export type DataflowIngestPipelineAdditionalMetadata = {
  ingestViewResultsDatasetName: string;
  stateResultsDatasetName: string;
};

export interface DataflowIngestRawDataWatermarks {
  [fileName: string]: Date;
}

export interface IngestViewResultRowCounts {
  [ingestViewName: string]: number;
}

export interface StateDatasetRowCounts {
  [dataset: string]: number;
}

export type DataflowIngestPipelineJobResponse = {
  [stateCode: string]: DataflowIngestPipelineStatus | null;
};

export enum IngestStatus {
  RAW_DATA_REIMPORT_STARTED = "RAW_DATA_REIMPORT_STARTED",
  INITIAL_STATE = "INITIAL_STATE",
  RAW_DATA_IMPORT_IN_PROGRESS = "RAW_DATA_IMPORT_IN_PROGRESS",
  READY_TO_FLASH = "READY_TO_FLASH",
  FLASH_IN_PROGRESS = "FLASH_IN_PROGRESS",
  FLASH_COMPLETED = "FLASH_COMPLETED",
  RAW_DATA_REIMPORT_CANCELED = "RAW_DATA_REIMPORT_CANCELED",
  RAW_DATA_REIMPORT_CANCELLATION_IN_PROGRESS = "RAW_DATA_REIMPORT_CANCELLATION_IN_PROGRESS",
  RAW_DATA_UP_TO_DATE = "RAW_DATA_UP_TO_DATE",
  STALE_RAW_DATA = "STALE_RAW_DATA",
  NO_RAW_DATA_REIMPORT_IN_PROGRESS = "NO_RAW_DATA_REIMPORT_IN_PROGRESS",

  // Legacy ingest raw data statuses which are still present in the operations DB
  RERUN_WITH_RAW_DATA_IMPORT_STARTED = "RERUN_WITH_RAW_DATA_IMPORT_STARTED",
  STANDARD_RERUN_STARTED = "STANDARD_RERUN_STARTED",
  BLOCKED_ON_PRIMARY_RAW_DATA_IMPORT = "BLOCKED_ON_PRIMARY_RAW_DATA_IMPORT",
  INGEST_VIEW_MATERIALIZATION_IN_PROGRESS = "INGEST_VIEW_MATERIALIZATION_IN_PROGRESS",
  EXTRACT_AND_MERGE_IN_PROGRESS = "EXTRACT_AND_MERGE_IN_PROGRESS",
  RERUN_CANCELED = "RERUN_CANCELED",
  RERUN_CANCELLATION_IN_PROGRESS = "RERUN_CANCELLATION_IN_PROGRESS",
  UP_TO_DATE = "UP_TO_DATE",
  NO_RERUN_IN_PROGRESS = "NO_RERUN_IN_PROGRESS",
}

export type IngestInstanceStatusInfo = {
  status: IngestStatus;
  statusTimestamp: string;
};

export type IngestInstanceStatusResponse = {
  [stateCode: string]: {
    primary: IngestInstanceStatusInfo;
    secondary: IngestInstanceStatusInfo;
  };
};

export enum QueueState {
  PAUSED = "PAUSED",
  RUNNING = "RUNNING",
  MIXED_STATUS = "MIXED_STATUS",
  UNKNOWN = "UNKNOWN",
}

export type QueueMetadata = {
  name: string;
  state: QueueState;
};

export type IngestInstanceResources = {
  storageDirectoryPath: string;
  ingestBucketPath: string;
};

export type IngestRawFileProcessingStatus = {
  fileTag: string;
  hasConfig: boolean;
  numberFilesInBucket: number;
  numberUnprocessedFiles: number;
  numberProcessedFiles: number;
  latestDiscoveryTime: string | null;
  latestProcessedTime: string | null;
  latestUpdateDatetime: string | null;
  isStale: boolean;
};

export type StateIngestQueuesStatuses = {
  [stateCode: string]: QueueState;
};

export const ANCHOR_DATAFLOW_LATEST_JOB = "dataflow_latest_job";
export const ANCHOR_INGEST_RAW_DATA = "ingest_raw_data";
export const ANCHOR_INGEST_RESOURCES = "ingest_resources";
export const ANCHOR_INGEST_LOGS = "ingest_logs";

export const FILE_TAG_UNNORMALIZED = "UNNORMALIZED"; // special tag for files that don't follow normalized format// Recidiviz - a data platform for criminal justice reform
export const FILE_TAG_IGNORED_IN_SUBDIRECTORY = "IGNORED_IN_SUBDIRECTORY"; // special tag for files that are not in the base directory

export const SPECIAL_FILE_TAGS = [
  FILE_TAG_UNNORMALIZED,
  FILE_TAG_IGNORED_IN_SUBDIRECTORY,
];
