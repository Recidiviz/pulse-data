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
import { PageHeader, Spin, Table } from "antd";
import * as React from "react";
import { fetchDataFreshness } from "../AdminPanelAPI";
import { useFetchedDataJSON } from "../hooks";

const DataFreshnessView = (): JSX.Element => {
  const { loading, data } =
    useFetchedDataJSON<DataFreshnessResult[]>(fetchDataFreshness);

  if (loading) {
    return (
      <div className="center">
        <Spin size="large" />
      </div>
    );
  }

  const columns = [
    {
      title: "State",
      dataIndex: "state",
      key: "state",
    },
    {
      title: "Date",
      dataIndex: "date",
      key: "date",
    },
    {
      title: "Notes",
      key: "notes",
      render: (_: string, record: DataFreshnessResult) => {
        if (!record.ingestPaused) {
          return "N/A";
        }
        return "BigQuery data refreshes have been paused. The data in this table may be incorrect as a result.";
      },
    },
  ];

  const sortedData = data?.sort((a, b) => (a.state > b.state ? 1 : -1));
  return (
    <>
      <PageHeader
        title="Data Freshness"
        subTitle="Indicates the high-water mark for content that has been ingested."
      />
      <Table
        columns={columns}
        dataSource={sortedData}
        pagination={{
          hideOnSinglePage: true,
          showSizeChanger: true,
          size: "small",
        }}
        rowKey="state"
      />
    </>
  );
};

export default DataFreshnessView;
