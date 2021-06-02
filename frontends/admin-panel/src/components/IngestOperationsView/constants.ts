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
export enum IngestActions {
  StartIngestRun = "start",
  PauseIngestQueues = "pause",
  ResumeIngestQueues = "resume",

  ExportToGCS = "export",
  ImportFromGCS = "import",
}

export const actionNames = {
  [IngestActions.StartIngestRun]: "Start Ingest Run",
  [IngestActions.PauseIngestQueues]: "Pause Queues",
  [IngestActions.ResumeIngestQueues]: "Resume Queues",

  [IngestActions.ExportToGCS]: "Export to GCS",
  [IngestActions.ImportFromGCS]: "Import from GCS",
};

export enum QueueState {
  PAUSED = "PAUSED",
  RUNNING = "RUNNING",
}

export type QueueMetadata = {
  name: string;
  state: QueueState;
};

export enum DirectIngestInstance {
  PRIMARY = "PRIMARY",
  SECONDARY = "SECONDARY",
}

export type IngestBucketSummary = {
  name: string;
  unprocessedFilesRaw: number;
  processedFilesRaw: number;
  unprocessedFilesIngestView: number;
  processedFilesIngestView: number;
};

export type IngestInstanceSummary = {
  instance: DirectIngestInstance;
  storage: string;
  ingest: IngestBucketSummary;
  dbName: string;
  operations: OperationsDbInfo;
};

export type OperationsDbInfo = {
  unprocessedFilesRaw: number;
  unprocessedFilesIngestView: number;
  dateOfEarliestUnprocessedIngestView: Date;
};

export type StateCodeInfo = {
  code: string;
  name: string;
};
