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
  IngestInstanceResources,
  IngestRawFileProcessingStatus,
} from "./constants";
import RawDataFileTagContents from "./RawDataFileTagContents";
import RawDataHasConfigFileCellContents from "./RawDataHasConfigFileCellContents";
import RawDataLatestProcessedDateCellContents from "./RawDataLatestProcessedDateCellContents";

interface IngestRawFileProcessingStatusTableProps {
  ingestInstanceResources: IngestInstanceResources | undefined;
  statusLoading: boolean;
  ingestRawFileProcessingStatus: IngestRawFileProcessingStatus[];
}

const IngestRawFileProcessingStatusTable: React.FC<
  IngestRawFileProcessingStatusTableProps
> = ({
  ingestInstanceResources,
  statusLoading,
  ingestRawFileProcessingStatus,
}) => {
  const columns: ColumnsType<IngestRawFileProcessingStatus> = [
    {
      title: "Raw Data File Tag",
      dataIndex: "fileTag",
      key: "fileTag",
      render: (_, record) => (
        <RawDataHasConfigFileCellContents status={record} />
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
      sortOrder: "descend",
    },
    {
      title: "Last Processed",
      dataIndex: "latestProcessedTime",
      key: "latestProcessedTime",
      render: (_, record) => (
        <RawDataLatestProcessedDateCellContents
          status={record}
          storageDirectoryPath={ingestInstanceResources?.storageDirectoryPath}
        />
      ),
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
        <RawDataFileTagContents
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
