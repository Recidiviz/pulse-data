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
import { Badge, Card, Divider, Table, Tooltip } from "antd";
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

interface RawDataImportStatusRow {
  importStatus: string;
  importStatusDescription: string;
}

const rawDataImportStatusColorDict: {
  [color: string]: RawDataImportStatusFormattingInfo;
} = {
  STARTED: {
    color: "default",
    sortRank: 1,
    status: "Started",
  },
  DEFERRED: {
    color: "default",
    sortRank: 2,
    status: "Deferred",
  },
  SUCCEEDED: {
    color: "success",
    sortRank: 3,
    status: "Succeeded",
  },
  FAILED_UNKNOWN: {
    color: "error",
    sortRank: 4,
    status: "Failed (Unknown Reason)",
  },
  FAILED_LOAD_STEP: {
    color: "error",
    sortRank: 5,
    status: "Failed BQ Load Step",
  },
  FAILED_PRE_IMPORT_NORMALIZATION_STEP: {
    color: "error",
    sortRank: 6,
    status: "Failed Pre-Import Normalization Step",
  },
  FAILED_VALIDATION_STEP: {
    color: "error",
    sortRank: 7,
    status: "Failed Pre-Import Validation Step",
  },
  FAILED_IMPORT_BLOCKED: {
    color: "error",
    sortRank: 7,
    status: "Failed Import Blocked",
  },
};

function renderRawDataImportStatus(importStatusRow: RawDataImportStatusRow) {
  const statusInfo = rawDataImportStatusColorDict[importStatusRow.importStatus];
  return (
    <Tooltip title={importStatusRow.importStatusDescription}>
      <Badge status={statusInfo.color} text={statusInfo.status} />
    </Tooltip>
  );
}

function rawDataImportStatusSortRank(importStatus: RawDataImportStatus) {
  return rawDataImportStatusColorDict[importStatus].sortRank;
}

const stagingDAGURL =
  "https://941882c9ef884317b481f986ea6d3a40-dot-us-central1.composer.googleusercontent.com/dags/recidiviz-staging_raw_data_import_dag/grid?dag_run_id=";
const prodDAGURL =
  "https://ef0f054ee6474821b3cd53e384c3e980-dot-us-central1.composer.googleusercontent.com/dags/recidiviz-123_raw_data_import_dag/grid?dag_run_id=";

function buildDagLink(dagRunId: string) {
  const urlBase =
    window.RUNTIME_GCP_ENVIRONMENT === "production"
      ? prodDAGURL
      : stagingDAGURL;

  return `${urlBase}${encodeURIComponent(dagRunId)}`;
}

function renderRunId(record: RawDataImportStartRow) {
  const dt = new Date(record.importRunStart);
  const timeAgo = moment(dt).fromNow();
  return (
    // TODO(trotto/go-links#245) return to a go link if they don't decode our values
    <NewTabLink href={buildDagLink(record.dagRunId)}>
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
    new Set<number | string | boolean>(
      items.map((x) => x[key]).filter((value) => value !== null) as (
        | number
        | string
        | boolean
      )[]
    )
  ).map((x) => ({
    text: `${x}`,
    value: x,
  }));
}

const RawDataFileTagRecentRunsTable: React.FC<
  RawDataFileTagRecentRunsTableProps
> = ({ importRuns, loading }) => {
  // Check if any import run has historical diffs active
  const hasHistoricalDiffsActive = importRuns.some(
    (run) => run.historicalDiffsActive
  );

  const baseColumns: ColumnsType<RawDataFileTagImport> = [
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
      dataIndex: ["importStatus", "importStatusDescription"],
      filters: filtersForKey("importStatus", importRuns),
      sorter: (a, b) =>
        rawDataImportStatusSortRank(a.importStatus) -
        rawDataImportStatusSortRank(b.importStatus),
      filterSearch: true,
      render: (_, record: RawDataImportStatusRow) =>
        renderRawDataImportStatus(record),
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

  const historicalDiffsColumns: ColumnsType<RawDataFileTagImport> = [
    {
      title: "Net New or Updated Rows",
      dataIndex: "netNewOrUpdatedRows",
      render: (netNewOrUpdatedRows: number | null) =>
        netNewOrUpdatedRows !== null ? netNewOrUpdatedRows : "-",
      sorter: (a, b) =>
        (a.netNewOrUpdatedRows || 0) - (b.netNewOrUpdatedRows || 0),
    },
    {
      title: "Deleted Rows",
      dataIndex: "deletedRows",
      render: (deletedRows: number | null) =>
        deletedRows !== null ? deletedRows : "-",
      sorter: (a, b) => (a.deletedRows || 0) - (b.deletedRows || 0),
    },
  ];

  // Combine columns conditionally
  const columns = [
    ...baseColumns.slice(0, -1), // All columns except "Is Invalidated"
    ...(hasHistoricalDiffsActive ? historicalDiffsColumns : []),
    baseColumns[baseColumns.length - 1], // "Is Invalidated" column at the end
  ];

  return (
    <>
      {importRuns ? (
        // TODO(#40151) add dynamic pagination?
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
