// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2022 Recidiviz, Inc.
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

export interface DirectIngestStatusFormattingInfo {
  status: string;
  color: string;
  sortRank: number;
}

const statusFormattingInfo: {
  [status: string]: DirectIngestStatusFormattingInfo;
} = {
  READY_TO_FLASH: {
    status: "READY_TO_FLASH",
    color: "ingest-status-cell-yellow",
    sortRank: 0,
  },
  EXTRACT_AND_MERGE_IN_PROGRESS: {
    status: "EXTRACT_AND_MERGE_IN_PROGRESS",
    color: "ingest-status-cell-grey",
    sortRank: 1,
  },
  FLASH_IN_PROGRESS: {
    status: "FLASH_IN_PROGRESS",
    color: "ingest-status-cell-grey",
    sortRank: 2,
  },
  INGEST_VIEW_MATERIALIZATION_IN_PROGRESS: {
    status: "INGEST_VIEW_MATERIALIZATION_IN_PROGRESS",
    color: "ingest-status-cell-grey",
    sortRank: 3,
  },
  RAW_DATA_IMPORT_IN_PROGRESS: {
    status: "RAW_DATA_IMPORT_IN_PROGRESS",
    color: "ingest-status-cell-grey",
    sortRank: 4,
  },
  RERUN_WITH_RAW_DATA_IMPORT_STARTED: {
    status: "RERUN_WITH_RAW_DATA_IMPORT_STARTED",
    color: "ingest-status-cell-grey",
    sortRank: 5,
  },
  STALE_RAW_DATA: {
    status: "STALE_RAW_DATA",
    color: "ingest-status-cell-grey",
    sortRank: 6,
  },
  STANDARD_RERUN_STARTED: {
    status: "STANDARD_RERUN_STARTED",
    color: "ingest-status-cell-grey",
    sortRank: 7,
  },
  FLASH_COMPLETED: {
    status: "FLASH_COMPLETED",
    color: "ingest-status-cell-green",
    sortRank: 8,
  },
  NO_RERUN_IN_PROGRESS: {
    status: "NO_RERUN_IN_PROGRESS",
    color: "ingest-status-cell-green",
    sortRank: 9,
  },
  UP_TO_DATE: {
    status: "UP_TO_DATE",
    color: "ingest-status-cell-green",
    sortRank: 10,
  },
  "No recorded statuses": {
    status: "No recorded statuses",
    color: "ingest-status-cell-yellow",
    sortRank: 11,
  },
};

export const getStatusBoxColor = (status: string): string => {
  return statusFormattingInfo[status].color;
};

export const getStatusSortedOrder = (): string[] => {
  return Object.values(statusFormattingInfo)
    .sort((info) => info.sortRank)
    .map((info) => info.status);
};
