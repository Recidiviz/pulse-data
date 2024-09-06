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
import { Badge, Card, Divider, Table } from "antd";
import { PresetStatusColorType } from "antd/es/_util/colors";
import { ColumnsType } from "antd/lib/table";
import { ColumnFilterItem } from "antd/lib/table/interface";
import moment from "moment";
import * as React from "react";

import NewTabLink from "../NewTabLink";
import { RawDataFileTagImport, RawDataImportStatus } from "./constants";

interface RawDataFileTagRecentRunsTableProps {
  importRuns: RawDataFileTagImport[];
  loading: boolean;
}

interface RawDataImportStatusFormattingInfo {
  color: PresetStatusColorType;
  sortRank: number;
  status: string;
}

interface RawDataImportStartRow {
  importRunStart: string;
  dagRunId: string;
  importRunId: number;
}

const rawDataImportStatusColorDict: {
  [color: string]: RawDataImportStatusFormattingInfo;
} = {
  STARTED: {
    color: "default",
    sortRank: 1,
    status: "Started",
  },
  SUCCEEDED: {
    color: "success",
    sortRank: 2,
    status: "Succeeded",
  },
  FAILED_UNKNOWN: {
    color: "error",
    sortRank: 3,
    status: "Failed (Unknown Reason)",
  },
  FAILED_LOAD_STEP: {
    color: "error",
    sortRank: 4,
    status: "Failed BQ Load Step",
  },
  FAILED_PRE_IMPORT_NORMALIZATION_STEP: {
    color: "error",
    sortRank: 5,
    status: "Failed Pre-Import Normalization Step",
  },
  FAILED_VALIDATION_STEP: {
    color: "error",
    sortRank: 6,
    status: "Failed Pre-Import Validation Step",
  },
};

// TODO(#29133) add hovering over to produce the descriptions from recidiviz/common/constants/operations/direct_ingest_raw_file_import.py
function renderRawDataImportStatus(importStatus: RawDataImportStatus) {
  const statusInfo = rawDataImportStatusColorDict[importStatus];
  return <Badge status={statusInfo.color} text={statusInfo.status} />;
}

function rawDataImportStatusSortRank(importStatus: RawDataImportStatus) {
  return rawDataImportStatusColorDict[importStatus].sortRank;
}

function renderRunId(record: RawDataImportStartRow) {
  const env =
    window.RUNTIME_GCP_ENVIRONMENT === "production" ? "prod" : "staging";
  const dt = new Date(record.importRunStart);
  const timeAgo = moment(dt).fromNow();
  return (
    <NewTabLink href={`http://go/raw-data-${env}-dag-run/${record.dagRunId}}`}>
      {`${record.importRunId} (${timeAgo})`}
    </NewTabLink>
  );
}

function getIsInvalidatedText(rec: RawDataFileTagImport) {
  if (rec.importStatus !== RawDataImportStatus.SUCCEEDED) {
    return "-";
  }
  return rec.isInvalidated ? "Yes" : "No";
}

function filtersForKey(
  key: keyof RawDataFileTagImport,
  items: RawDataFileTagImport[]
): ColumnFilterItem[] {
  return Array.from(
    new Set<number | string | boolean>(items.map((x) => x[key]))
  ).map((x) => ({
    text: `${x}`,
    value: x,
  }));
}

const RawDataFileTagRecentRunsTable: React.FC<
  RawDataFileTagRecentRunsTableProps
> = ({ importRuns, loading }) => {
  const columns: ColumnsType<RawDataFileTagImport> = [
    {
      title: "Run ID",
      dataIndex: ["importRunStart", "dagRunId", "importRunId"],
      render: (_, record: RawDataImportStartRow) => renderRunId(record),
      filters: filtersForKey("importRunId", importRuns),
      onFilter: (value, record: RawDataImportStartRow) =>
        record.importRunId === value,
      sorter: (a, b) => a.importRunId - b.importRunId,
      filterSearch: true,
    },
    {
      title: "Update Datetime",
      dataIndex: "updateDatetime",
      filters: filtersForKey("updateDatetime", importRuns),
      onFilter: (value, record: RawDataFileTagImport) =>
        record.updateDatetime === value,
      sorter: (a, b) =>
        new Date(a.updateDatetime).getTime() -
        new Date(b.updateDatetime).getTime(),
      filterSearch: true,
    },
    {
      title: "Import Status",
      dataIndex: "importStatus",
      filters: filtersForKey("importStatus", importRuns),
      render: (importStatus: RawDataImportStatus) =>
        renderRawDataImportStatus(importStatus),
      sorter: (a, b) =>
        rawDataImportStatusSortRank(a.importStatus) -
        rawDataImportStatusSortRank(b.importStatus),
      filterSearch: true,
    },
    {
      title: "Raw Row Count",
      dataIndex: "rawRowCount",
      render: (rawRowCount: number | undefined) => rawRowCount || "-",
      sorter: (a, b) => a.rawRowCount - b.rawRowCount,
    },
    {
      title: "Is Invalidated",
      dataIndex: ["isInvalidated", "importStatus"],
      filters: [
        { text: "Yes", value: "Yes" },
        { text: "No", value: "No" },
      ],
      render: (_, rec) => getIsInvalidatedText(rec),
      onFilter: (value, record: RawDataFileTagImport) =>
        getIsInvalidatedText(record) === value,
    },
  ];

  return (
    <>
      {importRuns ? (
        // TODO(#29133) add dynamic pagination?
        <>
          <Divider orientation="left"> Recent Raw File Imports</Divider>
          <Card>
            <Table
              columns={columns}
              dataSource={importRuns}
              loading={loading}
              rowKey={(record: RawDataFileTagImport) =>
                `${record.importRunId}_${record.fileId}`
              }
            />
          </Card>
        </>
      ) : null}
    </>
  );
};

export default RawDataFileTagRecentRunsTable;
