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
export enum QueueState {
  PAUSED = "PAUSED",
  RUNNING = "RUNNING",
}

export type QueueMetadata = {
  name: string;
  state: QueueState;
};

export type IngestInstanceStatusResponse = {
  [stateCode: string]: {
    primary?: string;
    secondary?: string;
  };
};

export enum DirectIngestInstance {
  PRIMARY = "PRIMARY",
  SECONDARY = "SECONDARY",
}

export type IngestInstanceSummary = {
  instance: DirectIngestInstance;
  storageDirectoryPath: string;
  ingestBucketPath: string;
  ingestBucketNumFiles: number;
  dbName: string;
  operations: OperationsDbInfo;
};

export type OperationsDbInfo = {
  isPaused: boolean;
  unprocessedFilesRaw: number;
  processedFilesRaw: number;

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
};

export type StateCodeInfo = {
  code: string;
  name: string;
};
