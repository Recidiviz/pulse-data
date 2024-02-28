// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2024 Recidiviz, Inc.
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
import { Alert, Spin, Table } from "antd";
import { ColumnsType } from "antd/lib/table";
import { observer } from "mobx-react-lite";
import moment from "moment";
import styled from "styled-components/macro";

import { InsightsConfiguration } from "../../InsightsStore/models/InsightsConfiguration";
import ConfigurationPresenter from "../../InsightsStore/presenters/ConfigurationPresenter";
import { optionalStringSort } from "../Utilities/GeneralUtilities";

const TableContainer = styled.div`
  padding-top: 1rem;
`;

const InsightsConfigurationsView = ({
  presenter,
}: {
  presenter: ConfigurationPresenter;
}): JSX.Element => {
  const { configs, hydrationState } = presenter;

  if (hydrationState.status === "loading") {
    return (
      <div className="center">
        <Spin size="large" />
      </div>
    );
  }

  if (hydrationState.status === "failed") {
    return (
      <div className="center">
        <Alert
          message="Error"
          description={hydrationState.error.message}
          type="error"
          closable
        />
      </div>
    );
  }

  if (hydrationState.status !== "hydrated" || !configs) {
    return <div />;
  }

  type ColumnData = { text: string; value: string | boolean };
  const filterData =
    (colData: InsightsConfiguration[]) =>
    (formatter: (item: InsightsConfiguration) => string): ColumnData[] =>
      colData
        .map((item) => formatter(item))
        .filter((v, i, a) => a.indexOf(v) === i)
        .sort()
        .map((item) => ({
          text: item,
          value: item,
        }));

  const metadataColumns: ColumnsType<InsightsConfiguration> = [
    {
      title: "ID",
      dataIndex: "id",
      key: "id",
      sorter: (a, b) => b.id - a.id,
      defaultSortOrder: "ascend",
      fixed: "left",
      width: 100,
    },
    {
      title: "Status",
      dataIndex: "status",
      key: "status",
      sorter: (a, b) => a.status.localeCompare(b.status),
      fixed: "left",
      width: 100,
      filters: [...filterData(configs)((c) => c.status)],
      onFilter: (
        value: string | boolean | number,
        record: InsightsConfiguration
      ) => {
        return record.status === (value as keyof InsightsConfiguration);
      },
    },
    {
      title: "Feature Variant",
      dataIndex: "featureVariant",
      key: "featureVariant",
      sorter: (a, b) => optionalStringSort(a.featureVariant, b.featureVariant),
      fixed: "left",
      width: 100,
    },
    {
      title: "Updated By",
      dataIndex: "updatedBy",
      key: "updatedBy",
      sorter: (a, b) => a.updatedBy.localeCompare(b.updatedBy),
    },
    {
      title: "Updated At",
      dataIndex: "updatedAt",
      key: "updatedAt",
      sorter: (a, b) => a.updatedAt.localeCompare(b.updatedAt),
      render: (date: string) => moment(date).format("lll"),
    },
  ];

  const copyColumnNames = Object.keys(configs[0]).filter(
    (d) =>
      !metadataColumns
        .map((c) => {
          return c.key;
        })
        .includes(d)
  );
  const copyColumns = copyColumnNames.map((c) => {
    return {
      title: c,
      dataIndex: c,
      key: c,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      sorter: (a: any, b: any) => a[c].localeCompare(b[c]),
    };
  });

  return (
    <TableContainer>
      <Table
        columns={metadataColumns.concat(copyColumns)}
        dataSource={configs}
        rowKey="id"
        scroll={{ x: 4000 }}
      />
    </TableContainer>
  );
};

export default observer(InsightsConfigurationsView);
