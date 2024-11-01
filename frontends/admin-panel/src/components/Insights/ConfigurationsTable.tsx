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
import { Link } from "react-router-dom";
import styled from "styled-components/macro";

import { InsightsConfiguration } from "../../InsightsStore/models/InsightsConfiguration";
import ConfigurationPresenter from "../../InsightsStore/presenters/ConfigurationPresenter";
import { optionalStringSort } from "../Utilities/GeneralUtilities";

const TableContainer = styled.div`
  padding-top: 1rem;
`;

const ConfigurationsTable = ({
  presenter,
  configs,
}: {
  presenter: ConfigurationPresenter;
  configs: InsightsConfiguration[];
}): JSX.Element => {
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

  // Create metadata columns which require special handling
  const metadataColumns: ColumnsType<InsightsConfiguration> = [
    {
      title: "Click to view details",
      dataIndex: "id",
      key: "id",
      sorter: (a, b) => b.id - a.id,
      fixed: "left",
      defaultSortOrder: "ascend",
      width: 60,
      render: (id) => (
        <Link
          to={`/admin/line_staff_tools/insights_configurations/${presenter.stateCode}/configurations/${id}`}
        >
          Configuration {id}
        </Link>
      ),
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
      width: 150,
    },
    {
      title: "Updated At",
      dataIndex: "updatedAt",
      key: "updatedAt",
      sorter: (a, b) => a.updatedAt.localeCompare(b.updatedAt),
      render: (date: string) => moment(date).format("lll"),
      width: 200,
    },
  ];

  return (
    <TableContainer>
      <Table columns={metadataColumns} dataSource={configs} rowKey="id" />
    </TableContainer>
  );
};

const ConfigurationsTableContainer = ({
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

  return <ConfigurationsTable presenter={presenter} configs={configs} />;
};

export default observer(ConfigurationsTableContainer);
