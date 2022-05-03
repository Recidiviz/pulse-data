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
import * as React from "react";
import { IngestInstanceSummary } from "./constants";

interface InstanceIngestViewMetadataProps {
  stateCode: string;
  data: IngestInstanceSummary;
}

const InstanceIngestViewMetadata: React.FC<InstanceIngestViewMetadataProps> = ({
  stateCode,
  data,
}) => {
  const materializationColumns = [
    {
      title: "Ingest View Name",
      dataIndex: "ingestViewName",
      key: "ingestViewName",
    },
    {
      title: () => {
        return (
          <span title="Number of raw data upload dates with generated results">
            Completed Jobs
          </span>
        );
      },
      dataIndex: "numCompletedJobs",
      key: "numCompletedJobs",
    },
    {
      title: () => {
        return (
          <span title="Number of raw data upload dates without generated results">
            Pending Jobs
          </span>
        );
      },
      dataIndex: "numPendingJobs",
      key: "numPendingJobs",
    },
    {
      title: () => {
        return (
          <span title="Timestamp associated with the raw data in the next pending job">
            Next Job (if applicable)
          </span>
        );
      },
      dataIndex: "pendingJobsMinDatetime",
      key: "pendingJobsMinDatetime",
    },
  ];

  const materializationTableData =
    data.operations.ingestViewMaterializationSummaries;

  const contentsColumns = [
    {
      title: "Ingest View Name",
      dataIndex: "ingestViewName",
      key: "ingestViewName",
    },
    {
      title: () => {
        return (
          <span title="Number of result rows that have been processed (committed to Postgres) for this view">
            Processed Rows
          </span>
        );
      },
      dataIndex: "numProcessedRows",
      key: "numProcessedRows",
    },
    {
      title: () => {
        return (
          <span title="Number of result rows that have yet to be processed (committed to Postgres) for this view">
            Unprocessed Rows
          </span>
        );
      },
      dataIndex: "numUnprocessedRows",
      key: "numUnprocessedRows",
    },
    {
      title: () => {
        return (
          <span title="Date of most recent raw data upload for which any results for this view have been processed (committed to Postgres)">
            Most Recently Processed (if applicable)
          </span>
        );
      },
      dataIndex: "processedRowsMaxDatetime",
      key: "processedRowsMaxDatetime",
    },
    {
      title: () => {
        return (
          <span title="Date of earliest raw data upload for which some results for this view have not yet been processed (committed to Postgres)">
            Next to Process (if applicable)
          </span>
        );
      },
      dataIndex: "unprocessedRowsMinDatetime",
      key: "unprocessedRowsMinDatetime",
    },
  ];

  const contentsTableData = data.operations.ingestViewContentsSummaries;

  return (
    <>
      <h3>Ingest View Results Generated</h3>
      <p>
        Describes which query results have been generated (i.e. written to the{" "}
        <code>{stateCode.toLowerCase()}_ingest_view_results</code> dataset in
        BQ) and are ready to be committed to the state schema.
      </p>
      <Table
        columns={materializationColumns}
        dataSource={materializationTableData}
        rowKey="ingestViewName"
        pagination={false}
        style={{ paddingBottom: 12 }}
      />
      <h3>Ingest View Results Transformed to State Schema</h3>
      <p>
        Describes which rows in the{" "}
        <code>{stateCode.toLowerCase()}_ingest_view_results</code> dataset have
        been processed (i.e. committed to the <code>state</code> schema).
      </p>
      <Table
        columns={contentsColumns}
        dataSource={contentsTableData}
        rowKey="ingestViewName"
        pagination={false}
      />
    </>
  );
};

export default InstanceIngestViewMetadata;
