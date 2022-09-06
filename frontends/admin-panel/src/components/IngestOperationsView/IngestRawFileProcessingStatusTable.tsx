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
import { Table } from "antd";
import { ColumnsType } from "antd/lib/table";
import classNames from "classnames";
import * as React from "react";
import {
  booleanSort,
  optionalBooleanSort,
  optionalNumberSort,
  optionalStringSort,
} from "../Utilities/GeneralUtilities";
import {
  FILE_TAG_IGNORED_IN_SUBDIRECTORY,
  FILE_TAG_UNNORMALIZED,
  IngestRawFileProcessingStatus,
} from "./constants";
import RawDataFileTagContents from "./IngestOperationsComponents/RawDataFileTagContents";
import RawDataHasConfigFileCellContents from "./IngestOperationsComponents/RawDataHasConfigFileCellContents";

interface IngestRawFileProcessingStatusTableProps {
  ingestRawFileProcessingStatus: IngestRawFileProcessingStatus[];
  ingestBucketPath: string;
  storageDirectoryPath: string;
}

const IngestRawFileProcessingStatusTable: React.FC<IngestRawFileProcessingStatusTableProps> =
  ({
    ingestRawFileProcessingStatus,
    ingestBucketPath,
    storageDirectoryPath,
  }) => {
    const allFilesLatestDiscoveryTime: Date | undefined =
      ingestRawFileProcessingStatus
        .filter((a) => a.latestDiscoveryTime !== null)
        .map(({ latestDiscoveryTime: x }) => new Date(x as string))
        .reduce(
          (a: Date | undefined, b: Date) => (a && a > b ? a : b),
          undefined
        );

    const isTooFarBeforeLatestDiscoveryTime: {
      [key: string]: boolean | undefined;
    } = ingestRawFileProcessingStatus.reduce(
      (previousValue, currentValue, i, all) => ({
        ...previousValue,
        [currentValue.fileTag]: calculateIsTooFarBeforeLatestDiscoveryTime(
          currentValue,
          allFilesLatestDiscoveryTime
        ),
      }),
      {}
    );

    const columns: ColumnsType<IngestRawFileProcessingStatus> = [
      {
        title: "Raw Data File Tag",
        dataIndex: "fileTag",
        key: "fileTag",
        render: (_, record) => (
          <RawDataHasConfigFileCellContents
            status={record}
            storageDirectoryPath={storageDirectoryPath}
          />
        ),
        sorter: {
          compare: (a, b) => a.fileTag.localeCompare(b.fileTag),
          multiple: 1,
        },
        defaultSortOrder: "ascend",
        filters: ingestRawFileProcessingStatus.map(({ fileTag }) => ({
          text: fileTag,
          value: fileTag,
        })),
        onFilter: (value, content) => content.fileTag === value,
        filterSearch: true,
      },
      {
        title: "Last Recieved",
        dataIndex: "latestDiscoveryTime",
        key: "latestDiscoveryTime",
        render: (_, record) =>
          renderLatestDiscoveryTime(
            record,
            isTooFarBeforeLatestDiscoveryTime[record.fileTag]
          ),
        sorter: (a, b) =>
          optionalStringSort(a.latestDiscoveryTime, b.latestDiscoveryTime),
      },
      {
        title: "Recieved 24 Hours Before Latest",
        key: "pastLatest",
        render: (_, record) =>
          renderIsTooFarBeforeLatestDiscoveryTime(
            isTooFarBeforeLatestDiscoveryTime[record.fileTag]
          ),
        sorter: {
          compare: (a, b) =>
            optionalBooleanSort(
              isTooFarBeforeLatestDiscoveryTime[a.fileTag],
              isTooFarBeforeLatestDiscoveryTime[b.fileTag]
            ),
          multiple: 3,
        },
      },
      {
        title: "Last Processed",
        dataIndex: "latestProcessedTime",
        key: "latestProcessedTime",
        render: (_, record) => renderLatestProcessedTime(record),
        sorter: (a, b) =>
          optionalStringSort(a.latestProcessedTime, b.latestProcessedTime),
      },
      {
        title: "# Pending Upload",
        dataIndex: "numberUnprocessedFiles",
        key: "numberUnprocessedFiles",
        sorter: {
          compare: (a, b) =>
            optionalNumberSort(
              a.numberUnprocessedFiles,
              b.numberUnprocessedFiles
            ),
          multiple: 2,
        },
        defaultSortOrder: "descend",
      },
      {
        title: "# Files in GCS Bucket",
        dataIndex: "numberFilesInBucket",
        key: "numberFilesInBucket",
        sorter: (a, b) =>
          optionalNumberSort(a.numberFilesInBucket, b.numberFilesInBucket),
      },
      {
        title: "Has Config",
        dataIndex: "hasConfig",
        key: "hasConfig",
        render: (_, record) => (
          <RawDataFileTagContents
            status={record}
            ingestBucketPath={ingestBucketPath}
          />
        ),
        sorter: {
          compare: (a, b) => booleanSort(a.hasConfig, b.hasConfig),
          multiple: 4,
        },
        defaultSortOrder: "ascend",
      },
    ];

    return (
      <Table
        dataSource={ingestRawFileProcessingStatus}
        columns={columns}
        rowKey="fileTag"
        pagination={{
          hideOnSinglePage: true,
          showSizeChanger: true,
          defaultPageSize: 50,
          pageSizeOptions: ["25", "50", "100", "500"],
          size: "small",
        }}
      />
    );
  };

export default IngestRawFileProcessingStatusTable;

function renderLatestDiscoveryTime(
  status: IngestRawFileProcessingStatus,
  tooFarPastLatest: boolean | undefined
) {
  return (
    <div className={classNames({ "ingest-danger": tooFarPastLatest })}>
      {status.latestDiscoveryTime}
    </div>
  );
}

function renderIsTooFarBeforeLatestDiscoveryTime(status: boolean | undefined) {
  if (typeof status !== "boolean") {
    return <div>N/A</div>;
  }
  return <div>{status ? "Yes" : "No"}</div>;
}

function calculateIsTooFarBeforeLatestDiscoveryTime(
  status: IngestRawFileProcessingStatus,
  allFilesLatestDiscoveryTime: Date | undefined
): boolean | undefined {
  if (!status.latestDiscoveryTime) {
    return;
  }

  if (!allFilesLatestDiscoveryTime) {
    return;
  }

  const discoveryTime = new Date(status.latestDiscoveryTime);

  const tooFarPastLatestDiscoveryTime =
    allFilesLatestDiscoveryTime.getTime() - discoveryTime.getTime() >
    1000 * 60 * 60 * 24; // over 24 hours past latest discovery time
  return tooFarPastLatestDiscoveryTime;
}

function renderLatestProcessedTime(status: IngestRawFileProcessingStatus) {
  const { fileTag, hasConfig, latestProcessedTime } = status;

  if (fileTag === FILE_TAG_IGNORED_IN_SUBDIRECTORY) {
    return <div className="ingest-caution">N/A - Ignored</div>;
  }

  if (fileTag === FILE_TAG_UNNORMALIZED) {
    <div className="ingest-danger">N/A - Unknown file tag</div>;
  }

  return (
    <div>
      {hasConfig ? latestProcessedTime : "N/A - No Raw Config File Available"}
    </div>
  );
}
