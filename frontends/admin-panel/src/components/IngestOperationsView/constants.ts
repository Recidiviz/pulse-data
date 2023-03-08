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

export const GCP_STORAGE_BASE_URL = `https://console.cloud.google.com/storage/browser/`;

export const FILE_TAG_IGNORED_IN_SUBDIRECTORY = "IGNORED_IN_SUBDIRECTORY"; // special tag for files that are not in the base directory
export const FILE_TAG_UNNORMALIZED = "UNNORMALIZED"; // special tag for files that don't follow normalized format
export const SPECIAL_FILE_TAGS = [
  FILE_TAG_UNNORMALIZED,
  FILE_TAG_IGNORED_IN_SUBDIRECTORY,
];

export const ANCHOR_INGEST_RAW_DATA = "ingest_raw_data";
export const ANCHOR_INGEST_VIEWS = "ingest_views";
export const ANCHOR_INGEST_RESOURCES = "ingest_resources";
export const ANCHOR_INGEST_LOGS = "ingest_logs";

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

export type IngestInstanceStatusInfo = {
  status: string;
  statusTimestamp: string;
};

export type IngestInstanceStatusResponse = {
  [stateCode: string]: {
    primary: IngestInstanceStatusInfo;
    secondary: IngestInstanceStatusInfo;
  };
};

export enum DirectIngestInstance {
  PRIMARY = "PRIMARY",
  SECONDARY = "SECONDARY",
}

export type IngestInstanceResources = {
  storageDirectoryPath: string;
  ingestBucketPath: string;
  dbName: string;
};

export type IngestViewSummaries = {
  ingestViewMaterializationSummaries: IngestViewMaterializationSummary[];
  ingestViewContentsSummaries: IngestViewContentsSummary[];
};

export type IngestViewMaterializationSummary = {
  ingestViewName: string;
  numPendingJobs: number;
  numCompletedJobs: number;
  completedJobsMaxDatetime: string | null;
  pendingJobsMinDatetime: string | null;
};

export type IngestViewContentsSummary = {
  ingestViewName: string;
  numUnprocessedRows: number;
  numProcessedRows: number;
  unprocessedRowsMinDatetime: string | null;
  processedRowsMaxDatetime: string | null;
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
};

export type StateCodeInfo = {
  code: string;
  name: string;
};

export type StateIngestQueuesStatuses = {
  [stateCode: string]: QueueState;
};

export type IngestInstanceStatusTableInfo = {
  stateCode: string;
  primary: string;
  secondary: string;
  queueInfo: string | undefined;
  timestampPrimary: string;
  timestampSecondary: string;
};
