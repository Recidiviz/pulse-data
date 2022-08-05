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
import * as React from "react";
import {
  booleanSort,
  optionalNumberSort,
  optionalStringSort,
} from "../Utilities/GeneralUtilities";
import { IngestRawFileProcessingStatus } from "./constants";

interface IngestRawFileProcessingStatusTableProps {
  ingestRawFileProcessingStatus: IngestRawFileProcessingStatus[];
}

const IngestRawFileProcessingStatusTable: React.FC<IngestRawFileProcessingStatusTableProps> =
  ({ ingestRawFileProcessingStatus }) => {
    const columns: ColumnsType<IngestRawFileProcessingStatus> = [
      {
        title: "Raw Data File Tag",
        dataIndex: "fileTag",
        key: "fileTag",
        sorter: {
          compare: (a, b) =>
            a.hasConfig === b.hasConfig
              ? a.fileTag.localeCompare(b.fileTag)
              : booleanSort(a.hasConfig, b.hasConfig),
          multiple: 2,
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
        sorter: (a, b) =>
          a.latestDiscoveryTime.localeCompare(b.latestDiscoveryTime),
      },
      {
        title: "Last Processed",
        dataIndex: "latestProcessedTime",
        key: "latestProcessedTime",
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
          multiple: 1,
        },
        defaultSortOrder: "descend",
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
