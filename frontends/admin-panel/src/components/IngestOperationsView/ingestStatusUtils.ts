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

import { GCP_STORAGE_BASE_URL } from "./constants";

export interface DirectIngestStatusFormattingInfo {
  status: string;
  color: string;
  sortRank: number;
  message: string;
}

const statusFormattingInfo: {
  [status: string]: DirectIngestStatusFormattingInfo;
} = {
  READY_TO_FLASH: {
    status: "READY_TO_FLASH",
    color: "ingest-status-cell-yellow",
    sortRank: 0,
    message:
      "Scheduler in SECONDARY found no more work to do - flash to PRIMARY is ready to take place",
  },
  EXTRACT_AND_MERGE_IN_PROGRESS: {
    status: "EXTRACT_AND_MERGE_IN_PROGRESS",
    color: "ingest-status-cell-grey",
    sortRank: 1,
    message:
      "Conversion of materialized ingest views to Postgres entities is in progress",
  },
  FLASH_IN_PROGRESS: {
    status: "FLASH_IN_PROGRESS",
    color: "ingest-status-cell-grey",
    sortRank: 2,
    message: "Flash of data from SECONDARY to PRIMARY is in progress",
  },
  INGEST_VIEW_MATERIALIZATION_IN_PROGRESS: {
    status: "INGEST_VIEW_MATERIALIZATION_IN_PROGRESS",
    color: "ingest-status-cell-grey",
    sortRank: 3,
    message: "Ingest view materialization is in progress",
  },
  RAW_DATA_IMPORT_IN_PROGRESS: {
    status: "RAW_DATA_IMPORT_IN_PROGRESS",
    color: "ingest-status-cell-grey",
    sortRank: 4,
    message: "Raw data import from GCS to BQ is in progress",
  },
  RERUN_WITH_RAW_DATA_IMPORT_STARTED: {
    status: "RERUN_WITH_RAW_DATA_IMPORT_STARTED",
    color: "ingest-status-cell-grey",
    sortRank: 5,
    message:
      "Rerun with both raw data import and ingest vew materialization has been kicked off",
  },
  STALE_RAW_DATA: {
    status: "STALE_RAW_DATA",
    color: "ingest-status-cell-grey",
    sortRank: 6,
    message:
      "Raw data in PRIMARY is more up to date than raw data in SECONDARY",
  },
  STANDARD_RERUN_STARTED: {
    status: "STANDARD_RERUN_STARTED",
    color: "ingest-status-cell-grey",
    sortRank: 7,
    message:
      "Standard rerun with only ingest view materialization has been kicked off",
  },
  FLASH_COMPLETED: {
    status: "FLASH_COMPLETED",
    color: "ingest-status-cell-green",
    sortRank: 8,
    message: "Flash of data from SECONDARY to PRIMARY is completed",
  },
  NO_RERUN_IN_PROGRESS: {
    status: "NO_RERUN_IN_PROGRESS",
    color: "ingest-status-cell-green",
    sortRank: 9,
    message: "No rerun is currently in progress in SECONDARY",
  },
  UP_TO_DATE: {
    status: "UP_TO_DATE",
    color: "ingest-status-cell-green",
    sortRank: 10,
    message: "Scheduler in PRIMARY found no more work to do and is up to date",
  },
  "No recorded statuses": {
    status: "No recorded statuses",
    color: "ingest-status-cell-yellow",
    sortRank: 11,
    message: "No recorded statuses",
  },
};

export const getStatusBoxColor = (status: string): string => {
  return statusFormattingInfo[status].color;
};

export const getStatusMessage = (status: string): string => {
  return statusFormattingInfo[status].message;
};

export const getStatusSortedOrder = (): string[] => {
  return Object.values(statusFormattingInfo)
    .sort((info) => info.sortRank)
    .map((info) => info.status);
};

export function getGCPBucketURL(
  fileDirectoryPath: string,
  fileTag: string
): string {
  return `${GCP_STORAGE_BASE_URL.concat(
    fileDirectoryPath
  )}?prefix=&forceOnObjectsSortingFiltering=true&pageState=(%22StorageObjectListTable%22:(%22f%22:%22%255B%257B_22k_22_3A_22_22_2C_22t_22_3A10_2C_22v_22_3A_22_5C_22${fileTag}_5C_22_22%257D%255D%22))`;
}
