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
import { PageHeader, Table, Button, Input, Space, Spin } from "antd";
import { FilterDropdownProps } from "antd/lib/table/interface";
import { SearchOutlined } from "@ant-design/icons";
import * as React from "react";
import { getStateUserPermissions } from "../AdminPanelAPI";
import { useFetchedDataJSON } from "../hooks";

const StateUserPermissionsView = (): JSX.Element => {
  const { loading, data } = useFetchedDataJSON<StateUserPermissionsResponse[]>(
    getStateUserPermissions
  );

  if (loading || !data) {
    return (
      <div className="center">
        <Spin size="large" />
      </div>
    );
  }

  const getColumnSearchProps = (dataIndex: string) => ({
    filterDropdown: ({
      setSelectedKeys,
      selectedKeys,
      confirm,
      clearFilters,
    }: FilterDropdownProps) => (
      <div style={{ padding: 8 }}>
        <Input
          placeholder="Search Emails"
          value={selectedKeys[0]}
          onChange={(e) =>
            setSelectedKeys(e.target.value ? [e.target.value] : [])
          }
          onPressEnter={() => confirm()}
          style={{
            marginBottom: 8,
            display: "block",
          }}
        />
        <Space>
          <Button
            type="primary"
            onClick={() => confirm()}
            icon={<SearchOutlined />}
            size="small"
            style={{
              width: 90,
            }}
          >
            Search
          </Button>
          <Button
            onClick={clearFilters}
            size="small"
            style={{
              width: 90,
            }}
          >
            Reset
          </Button>
        </Space>
      </div>
    ),
    filterIcon: (filtered: boolean) => (
      <SearchOutlined style={{ color: filtered ? "#1890ff" : undefined }} />
    ),
    onFilter: (
      value: string | number | boolean,
      record: StateUserPermissionsResponse
    ) =>
      record[dataIndex as keyof StateUserPermissionsResponse]
        .toString()
        .toLowerCase()
        .includes(value.toString().toLowerCase()),
  });

  const columns = [
    {
      title: "Email",
      dataIndex: "restrictedUserEmail",
      sorter: (
        a: StateUserPermissionsResponse,
        b: StateUserPermissionsResponse
      ) => a.restrictedUserEmail.localeCompare(b.restrictedUserEmail),
      ...getColumnSearchProps("restrictedUserEmail"),
    },
    {
      title: "State",
      dataIndex: "stateCode",
      filters: [
        {
          text: "CO",
          value: "US_CO",
        },
        {
          text: "ID",
          value: "US_ID",
        },
        {
          text: "ME",
          value: "US_ME",
        },
        {
          text: "MI",
          value: "US_MI",
        },
        {
          text: "MO",
          value: "US_MO",
        },
        {
          text: "ND",
          value: "US_ND",
        },
        {
          text: "TN",
          value: "US_TN",
        },
      ],
      onFilter: (
        value: string | number | boolean,
        record: StateUserPermissionsResponse
      ) =>
        record.stateCode.indexOf(
          value as keyof StateUserPermissionsResponse
        ) === 0,
      sorter: (
        a: StateUserPermissionsResponse,
        b: StateUserPermissionsResponse
      ) => a.stateCode.localeCompare(b.stateCode),
    },
    {
      title: "Allowed Supervision Location IDs",
      dataIndex: "allowedSupervisionLocationIds",
    },
    {
      title: "Allowed Supervision Location Level",
      dataIndex: "allowedSupervisionLocationLevel",
    },
    {
      title: "Can Access Leadership Dashboard",
      dataIndex: "canAccessLeadershipDashboard",
      render: (_: string, record: StateUserPermissionsResponse) => {
        return record.canAccessLeadershipDashboard.toString();
      },
    },
    {
      title: "Can Access Case Triage",
      dataIndex: "canAccessCaseTriage",
      render: (_: string, record: StateUserPermissionsResponse) => {
        return record.canAccessCaseTriage.toString();
      },
    },
    {
      title: "Routes",
      dataIndex: "routes",
      render: (_: string, record: StateUserPermissionsResponse) => {
        if (record.routes) {
          return JSON.stringify(record.routes, null, "\t").slice(2, -2);
        }
      },
    },
    {
      title: "Should See Beta Charts",
      dataIndex: "shouldSeeBetaCharts",
      render: (_: string, record: StateUserPermissionsResponse) => {
        return record.shouldSeeBetaCharts.toString();
      },
    },
  ];

  return (
    <>
      <PageHeader title="State User Permissions" />
      <Table dataSource={data} columns={columns} />
    </>
  );
};

export default StateUserPermissionsView;
