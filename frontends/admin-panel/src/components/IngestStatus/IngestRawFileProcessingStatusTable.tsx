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

import {
  booleanSort,
  optionalBooleanSort,
  optionalNumberSort,
  optionalStringSort,
} from "../Utilities/GeneralUtilities";
import {
  IngestInstanceResources,
  IngestRawFileProcessingStatus,
} from "./constants";
import RawDataFileTagCellContents from "./RawDataFileTagCellContents";
import RawDataHasConfigFileCellContents from "./RawDataHasConfigFileCellContents";
import RawDataLatestDatetimeCellContents from "./RawDataLatestProcessedDateCellContents";

interface IngestRawFileProcessingStatusTableProps {
  ingestInstanceResources: IngestInstanceResources | undefined;
  statusLoading: boolean;
  ingestRawFileProcessingStatus: IngestRawFileProcessingStatus[];
  stateCode: string;
  instance: string;
}

const IngestRawFileProcessingStatusTable: React.FC<
  IngestRawFileProcessingStatusTableProps
> = ({
  ingestInstanceResources,
  statusLoading,
  ingestRawFileProcessingStatus,
  stateCode,
  instance,
}) => {
  const columns: ColumnsType<IngestRawFileProcessingStatus> = [
    {
      title: "Raw Data File Tag",
      dataIndex: "fileTag",
      key: "fileTag",
      render: (_, record) => (
        <RawDataHasConfigFileCellContents
          status={record}
          stateCode={stateCode}
          instance={instance}
        />
      ),
      sorter: {
        compare: (a, b) => a.fileTag.localeCompare(b.fileTag),
      },

      filters: ingestRawFileProcessingStatus.map(({ fileTag }) => ({
        text: fileTag,
        value: fileTag,
      })),
      onFilter: (value, content) => content.fileTag === value,
      filterSearch: true,
    },
    {
      title: "Last Received",
      dataIndex: "latestDiscoveryTime",
      key: "latestDiscoveryTime",
      render: (_, record) => renderLatestDiscoveryTime(record, record.isStale),
      sorter: {
        compare: (a, b) =>
          optionalBooleanSort(a.isStale, b.isStale) &&
          optionalStringSort(a.latestDiscoveryTime, b.latestDiscoveryTime),
      },
    },
    {
      title: "Last Processed",
      dataIndex: "latestProcessedTime",
      key: "latestProcessedTime",
      render: (_, record) => (
        <RawDataLatestDatetimeCellContents
          datetime={record.latestProcessedTime}
          status={record}
          storageDirectoryPath={ingestInstanceResources?.storageDirectoryPath}
        />
      ),
      sorter: (a, b) =>
        optionalStringSort(a.latestProcessedTime, b.latestProcessedTime),
    },
    {
      title: "Max Update Datetime",
      dataIndex: "latestUpdateDatetime",
      key: "latestUpdateDatetime",
      render: (_, record) => (
        <RawDataLatestDatetimeCellContents
          datetime={record.latestUpdateDatetime}
          status={record}
          storageDirectoryPath={ingestInstanceResources?.storageDirectoryPath}
        />
      ),
      sorter: (a, b) =>
        optionalStringSort(a.latestUpdateDatetime, b.latestUpdateDatetime),
    },
    {
      title: "# Pending Upload",
      dataIndex: "numberUnprocessedFiles",
      key: "numberUnprocessedFiles",
      render: (_, record) => renderNumberUnprocessedFiles(record),
      sorter: {
        compare: (a, b) =>
          optionalNumberSort(
            a.numberUnprocessedFiles,
            b.numberUnprocessedFiles
          ),
      },
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
        <RawDataFileTagCellContents
          status={record}
          ingestBucketPath={ingestInstanceResources?.ingestBucketPath}
        />
      ),
      sorter: {
        compare: (a, b) => booleanSort(a.hasConfig, b.hasConfig),
      },
    },
  ];

  return (
    <Table
      dataSource={ingestRawFileProcessingStatus}
      loading={statusLoading}
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

function renderNumberUnprocessedFiles(status: IngestRawFileProcessingStatus) {
  const unGroupedText =
    status.numberUngroupedFiles === 0
      ? ""
      : ` (${status.numberUngroupedFiles} ungrouped chunks)`;
  return (
    <div
      className={classNames({
        "ingest-danger": status.numberUngroupedFiles !== 0,
      })}
    >
      {status.numberUnprocessedFiles}
      {unGroupedText}
    </div>
  );
}
