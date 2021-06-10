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
import { Empty, List, PageHeader, Spin, Table } from "antd";
import { ColumnsType, ColumnType } from "antd/es/table";
import * as React from "react";

import { fetchValidationStatus } from "../AdminPanelAPI";
import useFetchedData from "../hooks";
import uniqueStates from "./Utilities/UniqueStates";

interface MetadataItem {
  key: string;
  value: string;
}

const ValidationStatusView = (): JSX.Element => {
  const { loading, data } = useFetchedData<ValidationStatusResults>(
    fetchValidationStatus
  );

  if (data === undefined) {
    return <Empty className="buffer" />;
  }

  if (loading) {
    return (
      <div className="center">
        <Spin size="large" />
      </div>
    );
  }

  const records = Object.keys(data.results)
    .sort()
    .map((name: string): MetadataRecord<ValidationStatus> => {
      return {
        name,
        resultsByState: data.results[name],
      };
    });

  const labelColumns: ColumnsType<MetadataRecord<ValidationStatus>> = [
    {
      title: "Validation Name",
      key: "validation",
      fixed: "left",
      render: (_: string, record: MetadataRecord<ValidationStatus>) => (
        <div>{record.name}</div>
      ),
    },
  ];
  const allStates = uniqueStates(records);

  const columns = labelColumns.concat(
    allStates.map((s) => columnTypeForState(s))
  );

  const metadata: MetadataItem[] = [
    { key: "Run ID", value: data.runId },
    { key: "Run Date", value: data.runDate },
    { key: "System Version", value: data.systemVersion },
  ];
  return (
    <>
      <PageHeader
        title="Validation Status"
        subTitle="Shows the current status of each validation for each state."
      />
      <List
        size="small"
        dataSource={metadata}
        renderItem={(item: MetadataItem) => (
          <List.Item>
            {item.key}: {item.value}
          </List.Item>
        )}
      />
      <Table
        className="metadata-table"
        columns={columns}
        dataSource={records}
        pagination={{
          hideOnSinglePage: true,
          showSizeChanger: true,
          pageSize: 50,
          size: "small",
        }}
        rowClassName="metadata-table-row"
        rowKey="validation"
      />
    </>
  );
};

export default ValidationStatusView;

const columnTypeForState = (
  state: string
): ColumnType<MetadataRecord<ValidationStatus>> => {
  return {
    title: state,
    key: state,
    render: (_: string, record: MetadataRecord<ValidationStatus>) => {
      const status = record.resultsByState[state];
      if (status === undefined) {
        return <div>No Result</div>;
      }
      if (status.didRun === false) {
        return <div className="broken">Broken</div>;
      }
      if (status.hasData === false) {
        return <div>Need Data</div>;
      }
      if (!status.wasSuccessful) {
        return <div className="failed">Failed ({status.errorAmount})</div>;
      }
      return <div className="success">Passed ({status.errorAmount})</div>;
    },
  };
};
