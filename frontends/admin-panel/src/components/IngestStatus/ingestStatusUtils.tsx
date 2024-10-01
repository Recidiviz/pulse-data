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

import { Spin } from "antd";
import classNames from "classnames";
import moment from "moment";

import { GCP_STORAGE_BASE_URL } from "../general/constants";
import {
  DataflowIngestPipelineJobResponse,
  DataflowIngestPipelineStatus,
  DataflowJobState,
  DataflowJobStatusMetadata,
  QueueMetadata,
  QueueState,
  RawDataImportRunState,
  RawDataImportRunStatus,
  RawDataImportRunStatusInfo,
  RawDataResourceLockActor,
  RawDataResourceLockStatuses,
  ResourceLockState,
  ResourceLockStatus,
} from "./constants";

export interface DirectIngestCellFormattingInfo {
  color: string;
  sortRank: number;
}

export interface DirectIngestCellFormattingAndStatusInfo
  extends DirectIngestCellFormattingInfo {
  color: string;
  sortRank: number;
  status: string;
}

export interface DirectIngestStatusFormattingInfo
  extends DirectIngestCellFormattingAndStatusInfo {
  message: string;
}

// TODO(#28239): remove once the raw data import dag is fully rolled out
// --- legacy ingest instance status utils ---------------------------------------------

const legacyIngestStatusFormattingInfo: {
  [status: string]: DirectIngestStatusFormattingInfo;
} = {
  READY_TO_FLASH: {
    status: "READY_TO_FLASH",
    color: "ingest-status-cell-yellow",
    sortRank: 0,
    message:
      "Scheduler in SECONDARY found no more work to do - flash to PRIMARY is ready to take place",
  },
  FLASH_IN_PROGRESS: {
    status: "FLASH_IN_PROGRESS",
    color: "ingest-status-cell-grey",
    sortRank: 2,
    message: "Flash of data from SECONDARY to PRIMARY is in progress",
  },
  RAW_DATA_REIMPORT_CANCELLATION_IN_PROGRESS: {
    status: "RAW_DATA_REIMPORT_CANCELLATION_IN_PROGRESS",
    color: "ingest-status-cell-grey",
    sortRank: 4,
    message: "Cancellation of raw data reimport in SECONDARY is in progress",
  },
  RAW_DATA_IMPORT_IN_PROGRESS: {
    status: "RAW_DATA_IMPORT_IN_PROGRESS",
    color: "ingest-status-cell-grey",
    sortRank: 6,
    message: "Raw data import from GCS to BQ is in progress",
  },
  RAW_DATA_REIMPORT_STARTED: {
    status: "RAW_DATA_REIMPORT_STARTED",
    color: "ingest-status-cell-grey",
    sortRank: 9,
    message: "A reimport of raw data in SECONDARY has been kicked off",
  },
  STALE_RAW_DATA: {
    status: "STALE_RAW_DATA",
    color: "ingest-status-cell-grey",
    sortRank: 10,
    message:
      "Raw data in PRIMARY is more up to date than raw data in SECONDARY",
  },
  INITIAL_STATE: {
    status: "INITIAL_STATE",
    color: "ingest-status-cell-grey",
    sortRank: 12,
    message:
      "Raw data import has been enabled in PRIMARY but nothing has processed yet",
  },
  RAW_DATA_REIMPORT_CANCELED: {
    status: "RAW_DATA_REIMPORT_CANCELED",
    color: "ingest-status-cell-grey",
    sortRank: 14,
    message: "Raw data reimport in SECONDARY has been canceled",
  },
  FLASH_COMPLETED: {
    status: "FLASH_COMPLETED",
    color: "ingest-status-cell-green",
    sortRank: 15,
    message: "Flash of data from SECONDARY to PRIMARY is completed",
  },
  NO_RAW_DATA_REIMPORT_IN_PROGRESS: {
    status: "NO_RAW_DATA_REIMPORT_IN_PROGRESS",
    color: "ingest-status-cell-green",
    sortRank: 17,
    message: "No raw data reimport is currently in progress in SECONDARY",
  },
  RAW_DATA_UP_TO_DATE: {
    status: "RAW_DATA_UP_TO_DATE",
    color: "ingest-status-cell-green",
    sortRank: 19,
    message:
      "Scheduler in PRIMARY found no more raw data import work to do and is up to date",
  },
};

export const getLegacyIngestStatusBoxColor = (status: string): string => {
  return legacyIngestStatusFormattingInfo[status].color;
};

export const getLegacyIngestStatusMessage = (
  status: string,
  timestamp: string
): string => {
  const dt = new Date(timestamp);
  const timeAgo = moment(dt).fromNow();
  return `${legacyIngestStatusFormattingInfo[status].message} (${timeAgo})`;
};

export const getLegacyIngestStatusSortedOrder = (): string[] => {
  return Object.values(legacyIngestStatusFormattingInfo)
    .sort((info) => info.sortRank)
    .map((info) => info.status);
};

export const renderLegacyIngestStatusCell = (
  status: string | undefined,
  timestamp: string | undefined
): React.ReactElement => {
  if (status === undefined || timestamp === undefined) return <Spin />;
  const statusColorClassName = getLegacyIngestStatusBoxColor(status);
  const statusMessage = getLegacyIngestStatusMessage(status, timestamp);

  return (
    <div className={classNames("ingest-status-cell", statusColorClassName)}>
      {statusMessage}
    </div>
  );
};

// --- dataflow pipeline status utils --------------------------------------------------

const jobStateColorDict: {
  [color: string]: DirectIngestCellFormattingInfo;
} = {
  SUCCEEDED: {
    color: "job-succeeded",
    sortRank: 1,
  },
  FAILED: {
    color: "job-failed",
    sortRank: 2,
  },
  NO_JOB_RUNS: {
    color: "job-no-runs",
    sortRank: 3,
  },
  NOT_ENABLED: {
    color: "job-dataflow-not-enabled",
    sortRank: 4,
  },
};

function getCurrentStatus(
  pipelineStatus: DataflowIngestPipelineStatus | null
): DataflowJobState {
  if (pipelineStatus == null) {
    return DataflowJobState.NO_JOB_RUNS;
  }
  if (pipelineStatus.terminationState === "JOB_STATE_DONE") {
    return DataflowJobState.SUCCEEDED;
  }
  if (pipelineStatus.terminationState === "JOB_STATE_FAILED") {
    return DataflowJobState.FAILED;
  }
  throw new Error("Unknown job state found");
}

function getDataflowJobStateColor(currentState: DataflowJobState): string {
  return jobStateColorDict[currentState].color;
}

// for primary only
export function getJobMetadataForCell(
  key: string,
  pipelineStatuses: DataflowIngestPipelineJobResponse | undefined
): DataflowJobStatusMetadata | undefined {
  if (pipelineStatuses === undefined) return undefined;
  return {
    status: getCurrentStatus(pipelineStatuses[key]),
    terminationTime: pipelineStatuses[key]?.terminationTime,
  };
}

export const getDataflowEnabledSortedOrder = (
  dataflowEnabled: boolean | undefined
): number => {
  if (!dataflowEnabled) {
    return 0;
  }
  return dataflowEnabled ? 1 : 0;
};

export const renderDataflowStatusCell = (
  statusMetadata: DataflowJobStatusMetadata | undefined
) => {
  if (statusMetadata === undefined) return <Spin />;
  return (
    <div
      className={classNames(getDataflowJobStateColor(statusMetadata.status))}
    >
      {statusMetadata.status}
      {statusMetadata.terminationTime
        ? `\n(${moment(
            new Date(statusMetadata.terminationTime * 1000)
          ).fromNow()})`
        : null}
    </div>
  );
};

// TODO(#28239): remove once the raw data import dag is fully rolled out
// --- ingest queue status utils -------------------------------------------------------

const queueStatusColorDict: {
  [color: string]: DirectIngestCellFormattingInfo;
} = {
  PAUSED: {
    color: "queue-status-not-running",
    sortRank: 1,
  },
  MIXED_STATUS: {
    color: "queue-status-not-running",
    sortRank: 2,
  },
  UNKNOWN: {
    color: "queue-status-not-running",
    sortRank: 3,
  },
  RUNNING: {
    color: "queue-status-running",
    sortRank: 4,
  },
};

export const getQueueColor = (queueInfo: string): string => {
  return queueStatusColorDict[queueInfo].color;
};

export const getQueueStatusSortedOrder = (
  queueInfo: string | undefined
): number => {
  if (!queueInfo) {
    return 0;
  }

  return queueStatusColorDict[queueInfo].sortRank;
};

export function getIngestQueuesCumulativeState(
  queueInfos: QueueMetadata[]
): QueueState {
  return queueInfos
    .map((queueInfo) => queueInfo.state)
    .reduce((acc: QueueState, state: QueueState) => {
      if (acc === QueueState.UNKNOWN) {
        return state;
      }
      if (acc === state) {
        return acc;
      }
      return QueueState.MIXED_STATUS;
    }, QueueState.UNKNOWN);
}

export const renderIngestQueuesCell = (queueInfo: string | undefined) => {
  if (queueInfo === undefined) {
    return <Spin />;
  }
  const queueColor = getQueueColor(queueInfo);

  return <div className={classNames(queueColor)}>{queueInfo}</div>;
};

// --- raw data import dag status utils ------------------------------------------------

const rawDataImportRunColorStatusDict: {
  [color: string]: DirectIngestCellFormattingInfo;
} = {
  SUCCEEDED: {
    color: "raw-data-import-success",
    sortRank: 1,
  },
  HAS_FAILURE: {
    color: "raw-data-import-failure",
    sortRank: 2,
  },
  IN_PROGRESS: {
    color: "raw-data-import-in-progress",
    sortRank: 3,
  },
  NO_JOB_RUNS: {
    color: "raw-data-import-no-runs",
    sortRank: 4,
  },
  NOT_ENABLED: {
    color: "raw-data-import-not-enabled",
    sortRank: 5,
  },
};

function getColorStatusForInfo(
  rawDataImportRunStatus: RawDataImportRunStatusInfo
): DirectIngestCellFormattingInfo {
  if (!rawDataImportRunStatus.isEnabled) {
    return rawDataImportRunColorStatusDict.NOT_ENABLED;
  }

  if (!rawDataImportRunStatus.importRunStart) {
    return rawDataImportRunColorStatusDict.NO_JOB_RUNS;
  }

  const hasInProgress = rawDataImportRunStatus.countByStatusBucket.reduce(
    (reduced, val) =>
      reduced || val.importStatus === RawDataImportRunState.IN_PROGRESS,
    false
  );

  if (hasInProgress) {
    return rawDataImportRunColorStatusDict.IN_PROGRESS;
  }

  const hasFailures = rawDataImportRunStatus.countByStatusBucket.reduce(
    (reduced, val) =>
      reduced || val.importStatus === RawDataImportRunState.FAILED,
    false
  );

  if (hasFailures) {
    return rawDataImportRunColorStatusDict.HAS_FAILURE;
  }

  return rawDataImportRunColorStatusDict.SUCCEEDED;
}

function getRawDataImportRunStatusColor(
  rawDataImportRunStatus: RawDataImportRunStatusInfo
): string {
  return getColorStatusForInfo(rawDataImportRunStatus).color;
}

const getRawDataFileStatusMessage = (
  runStatus: RawDataImportRunStatus
): string => {
  const plural = runStatus.fileCount === 1 ? "" : "s";
  return `${runStatus.fileCount} file${plural} ${runStatus.importStatus}`;
};

const getRawDataStatusMessage = (
  rawDataImportRunStatus: RawDataImportRunStatusInfo
): string => {
  let statusString = "";
  if (!rawDataImportRunStatus.isEnabled) {
    statusString = "Not Enabled";
  } else if (!rawDataImportRunStatus.importRunStart) {
    statusString = "No Runs Found";
  } else {
    const dt = new Date(rawDataImportRunStatus.importRunStart);
    const timeAgo = moment(dt).fromNow();
    const runStatusString = rawDataImportRunStatus.countByStatusBucket.reduce(
      (reduced, val) => `${reduced}${getRawDataFileStatusMessage(val)}\n`,
      ""
    );
    statusString = `${runStatusString} (${timeAgo})`;
  }
  return statusString;
};

export const renderRawDataImportRunStatusCell = (
  rawDataImportRunStatus: RawDataImportRunStatusInfo | undefined
) => {
  if (rawDataImportRunStatus === undefined) return <Spin />;
  return (
    <div
      className={classNames(
        "raw-data-import-cell",
        getRawDataImportRunStatusColor(rawDataImportRunStatus)
      )}
    >
      {getRawDataStatusMessage(rawDataImportRunStatus)}
    </div>
  );
};

export function getRawDataImportRunStatusSortedOrder(
  rawDataImportRunStatus: RawDataImportRunStatusInfo
): number {
  return getColorStatusForInfo(rawDataImportRunStatus).sortRank;
}

// --- raw data resource lock status utils ---------------------------------------------

const rawDataResourceLockColorStatusDict: {
  [color: string]: DirectIngestCellFormattingAndStatusInfo;
} = {
  BLOCKING: {
    status: "RAW DATA IMPORT BLOCKED BY MANUAL PROCESS",
    color: "resource-locks-held",
    sortRank: 1,
  },
  NOT_BLOCKING: {
    status: "No manual holds",
    color: "resource-locks-free",
    sortRank: 2,
  },
};

const getRawDataResourceLockStatusMessage = (
  rawDataResourceLockStatus: RawDataResourceLockStatuses
): string => {
  return getRawDataResourceLockStateForStatuses(rawDataResourceLockStatus)
    .status;
};

function getRawDataResourceLockStateColor(
  rawDataResourceLockStatus: RawDataResourceLockStatuses
): string {
  return getRawDataResourceLockStateForStatuses(rawDataResourceLockStatus)
    .color;
}

function getRawDataResourceLockStateForStatuses(
  rawDataResourceLockStatus: RawDataResourceLockStatuses
): DirectIngestCellFormattingAndStatusInfo {
  const hasManualHold = Object.values(rawDataResourceLockStatus).reduce(
    (result, holder) => result || holder === RawDataResourceLockActor.ADHOC,
    false
  );
  return hasManualHold
    ? rawDataResourceLockColorStatusDict.BLOCKING
    : rawDataResourceLockColorStatusDict.NOT_BLOCKING;
}

export function getRawDataResourceLockStateSortedOrder(
  rawDataResourceLockStatus: RawDataResourceLockStatuses
): number {
  return getRawDataResourceLockStateForStatuses(rawDataResourceLockStatus)
    .sortRank;
}

export const renderRawDataResourceLockStatusesCell = (
  rawDataResourceLockStatus: RawDataResourceLockStatuses | undefined
) => {
  if (rawDataResourceLockStatus === undefined) return <Spin />;
  return (
    <div
      className={classNames(
        "resource-lock-cell",
        getRawDataResourceLockStateColor(rawDataResourceLockStatus)
      )}
    >
      {getRawDataResourceLockStatusMessage(rawDataResourceLockStatus)}
    </div>
  );
};

const ResourceLockStatusColorDict: {
  [color: string]: DirectIngestCellFormattingInfo;
} = {
  ADHOC_HELD: {
    color: "resource-locks-held",
    sortRank: 1,
  },
  PROCESS_HELD: {
    color: "resource-locks-held",
    sortRank: 1,
  },
  MIXED: {
    color: "resource-locks-mixed",
    sortRank: 2,
  },
  UNKNOWN: {
    color: "resource-locks-unknown",
    sortRank: 3,
  },
  FREE: {
    color: "resource-locks-free",
    sortRank: 4,
  },
};

export function getResourceLockColor(state: string) {
  return ResourceLockStatusColorDict[state].color;
}

export function getResourceLockAdHocCumulativeState(
  lockStatuses: ResourceLockStatus[]
): ResourceLockState {
  return lockStatuses
    .map((lockStatus: ResourceLockStatus) => {
      if (lockStatus.released) return ResourceLockState.FREE;
      switch (lockStatus.actor) {
        case RawDataResourceLockActor.ADHOC:
          return ResourceLockState.ADHOC_HELD;
        case RawDataResourceLockActor.PROCESS:
          return ResourceLockState.PROCESS_HELD;
        default:
          throw new Error("Unrecognized actor");
      }
    })
    .reduce((acc: ResourceLockState, lockState: ResourceLockState) => {
      if (acc === ResourceLockState.UNKNOWN) {
        return lockState;
      }
      if (acc === lockState) {
        return acc;
      }
      return ResourceLockState.MIXED;
    }, ResourceLockState.UNKNOWN);
}

// --- misc status utils ---------------------------------------------------------------

export function getGCPBucketURL(
  fileDirectoryPath: string,
  fileTag: string
): string {
  return `${GCP_STORAGE_BASE_URL.concat(
    fileDirectoryPath
  )}?prefix=&forceOnObjectsSortingFiltering=true&pageState=(%22StorageObjectListTable%22:(%22f%22:%22%255B%257B_22k_22_3A_22_22_2C_22t_22_3A10_2C_22v_22_3A_22_5C_22${fileTag}_5C_22_22%257D%255D%22))`;
}

export function removeUnderscore(a: string): string {
  return a.replaceAll("_", " ");
}
