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

export enum JobState {
  SUCCEEDED = "SUCCEEDED",
  FAILED = "FAILED",
  NO_JOB_RUNS = "NO_JOB_RUNS",
  NOT_ENABLED = "NOT_ENABLED",
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
  [stateCode: string]: {
    primary: DataflowIngestPipelineStatus | null;
    secondary: DataflowIngestPipelineStatus | null;
  };
};

export const ANCHOR_DATAFLOW_LATEST_JOB = "dataflow_latest_job";
