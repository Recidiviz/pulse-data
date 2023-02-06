// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2023 Recidiviz, Inc.
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
import { Button, FormInstance, Input, Space, Typography } from "antd";
import * as React from "react";
import { ColumnType, FilterDropdownProps } from "antd/lib/table/interface";
import { SearchOutlined } from "@ant-design/icons";
import { Routes } from "../constants";

export const updatePermissionsObject = (
  existingRoutes: Routes,
  updatedRoutes: Partial<StateUserPermissionsResponse>,
  validPermissions: Record<string, string>
): Routes | undefined => {
  const newRoutes = Object.entries(updatedRoutes).reduce(
    (permissions, [permissionType, permissionValue]) => {
      if (
        Object.keys(validPermissions).includes(permissionType) &&
        permissionValue !== undefined
      ) {
        return {
          ...permissions,
          // If the permission type is a route or feature variant (requirement of this logic branch),
          // the value will be a boolean so typecasting is safe
          [permissionType]: permissionValue as boolean,
        };
      }
      return { ...permissions };
    },
    existingRoutes
  );
  return Object.keys(newRoutes).length > 0 ? newRoutes : undefined;
};

export const checkResponse = async (response: Response): Promise<void> => {
  if (!response.ok) {
    const error = await response.text();
    throw error;
  }
};

export function validateAndFocus<Param>(
  form: FormInstance,
  thenFunc: (values: Param) => void
): void {
  form
    .validateFields()
    .then(thenFunc)
    .catch((errorInfo) => {
      // hypothetically setting `scrollToFirstError` on the form should do this (or at least
      // scroll so the error is visible), but it doesn't seem to, so instead put the cursor in the
      // input directly.
      document.getElementById(errorInfo.errorFields?.[0].name?.[0])?.focus();
    });
}

const { Text } = Typography;
export const formatText = (
  text: string | boolean,
  record: StateUserPermissionsResponse
): string | boolean | JSX.Element => {
  if (record.blocked === true) {
    return (
      <Text type="secondary" italic>
        {text}
      </Text>
    );
  }
  return text;
};

const getColumnSearchProps = (dataIndex: string) => ({
  filterDropdown: ({
    setSelectedKeys,
    selectedKeys,
    confirm,
    clearFilters,
  }: FilterDropdownProps) => (
    <div style={{ padding: 8 }}>
      <Input
        placeholder="Search..."
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
          onClick={() => {
            clearFilters?.();
            confirm();
          }}
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
  ) => {
    const item = record[dataIndex as keyof StateUserPermissionsResponse];
    return typeof item !== "object"
      ? item.toString().toLowerCase().includes(value.toString().toLowerCase())
      : JSON.stringify(item)
          .toLowerCase()
          .includes(value.toString().toLowerCase());
  },
});

type ColumnData = { text: string; value: string | boolean };
export const filterData =
  (colData: StateUserPermissionsResponse[]) =>
  (formatter: (item: StateUserPermissionsResponse) => string): ColumnData[] =>
    colData
      .map((item) => formatter(item))
      .filter((v, i, a) => a.indexOf(v) === i)
      .map((item) => ({
        text: item,
        value: item,
      }));

export const getPermissionsTableColumns = (
  data: StateUserPermissionsResponse[],
  stateRoleData: StateRolePermissionsResponse[],
  setUserToEnable?: (user: StateUserPermissionsResponse) => void
): ColumnType<StateUserPermissionsResponse>[] => {
  return [
    {
      title: "State",
      dataIndex: "stateCode",
      key: "stateCode",
      width: 100,
      filters: [...filterData(data)((d) => d.stateCode)],
      onFilter: (
        value: string | number | boolean,
        record: StateUserPermissionsResponse
      ) => {
        return (
          record.stateCode?.indexOf(
            value as keyof StateUserPermissionsResponse
          ) === 0
        );
      },
      sorter: (
        a: StateUserPermissionsResponse,
        b: StateUserPermissionsResponse
      ) => a.stateCode.localeCompare(b.stateCode),
      render: (text, record) => {
        return formatText(text, record);
      },
    },
    {
      title: "Email",
      dataIndex: "emailAddress",
      key: "emailAddress",
      width: 250,
      sorter: (
        a: StateUserPermissionsResponse,
        b: StateUserPermissionsResponse
      ) => a.emailAddress.localeCompare(b.emailAddress),
      ...getColumnSearchProps("emailAddress"),
      render: (text, record) => {
        return formatText(text, record);
      },
    },
    {
      title: "First Name",
      dataIndex: "firstName",
      key: "firstName",
      width: 200,
      render: (text, record) => {
        return formatText(text, record);
      },
    },
    {
      title: "Last Name",
      dataIndex: "lastName",
      key: "lastName",
      width: 200,
      render: (text, record) => {
        return formatText(text, record);
      },
    },
    {
      title: "External ID",
      dataIndex: "externalId",
      width: 200,
      ...getColumnSearchProps("externalId"),
      render: (text: string, record) => {
        return formatText(text, record);
      },
    },
    {
      title: "Role",
      dataIndex: "role",
      key: "role",
      width: 150,
      filters: [...filterData(data)((d) => d.role)],
      onFilter: (
        value: string | number | boolean,
        record: StateUserPermissionsResponse
      ) => {
        return (
          record.role?.indexOf(value as keyof StateUserPermissionsResponse) ===
          0
        );
      },
      sorter: (
        a: StateUserPermissionsResponse,
        b: StateUserPermissionsResponse
      ) => a.role?.localeCompare(b.role),
      render: (text, record) => {
        return formatText(text, record);
      },
    },
    {
      title: "Routes",
      dataIndex: "routes",
      key: "routes",
      width: 300,
      ...getColumnSearchProps("routes"),
      render: (
        text: Record<string, unknown>,
        record: StateUserPermissionsResponse
      ) => {
        const role = stateRoleData.find(
          (d) => d.stateCode === record.stateCode && d.role === record.role
        );
        return {
          props: {
            style: {
              background:
                JSON.stringify(role?.routes) === JSON.stringify(record.routes)
                  ? "none"
                  : "yellow",
            },
          },
          children: formatText(
            JSON.stringify(text, null, "\t").slice(2, -2),
            record
          ),
        };
      },
    },
    {
      title: "Feature variants",
      dataIndex: "featureVariants",
      key: "featureVariants",
      width: 350,
      ...getColumnSearchProps("featureVariants"),
      render: (
        text: Record<string, unknown>,
        record: StateUserPermissionsResponse
      ) => {
        const role = stateRoleData.find(
          (d) => d.stateCode === record.stateCode && d.role === record.role
        );
        return {
          props: {
            style: {
              background:
                JSON.stringify(role?.featureVariants) ===
                JSON.stringify(record.featureVariants)
                  ? "none"
                  : "yellow",
            },
          },
          children: formatText(
            JSON.stringify(text, null, "\t").slice(2, -2),
            record
          ),
        };
      },
    },
    {
      title: "Can Access Leadership Dashboard",
      dataIndex: "canAccessLeadershipDashboard",
      key: "canAccessLeadershipDashboard",
      width: 180,
      filters: [
        {
          value: true,
          text: "true",
        },
        {
          value: false,
          text: "false",
        },
      ],
      onFilter: (
        value: boolean | string | number,
        record: StateUserPermissionsResponse
      ) => {
        return record.canAccessLeadershipDashboard === value;
      },
      sorter: (
        a: StateUserPermissionsResponse,
        b: StateUserPermissionsResponse
      ) =>
        Number(a.canAccessLeadershipDashboard) -
        Number(b.canAccessLeadershipDashboard),
      render: (text, record) => {
        if (text != null) {
          return formatText(text.toString(), record);
        }
      },
    },
    {
      title: "Can Access Case Triage",
      dataIndex: "canAccessCaseTriage",
      key: "canAccessCaseTriage",
      width: 180,
      filters: [
        {
          value: true,
          text: "true",
        },
        {
          value: false,
          text: "false",
        },
      ],
      onFilter: (
        value: boolean | string | number,
        record: StateUserPermissionsResponse
      ) => {
        return record.canAccessCaseTriage === value;
      },
      sorter: (
        a: StateUserPermissionsResponse,
        b: StateUserPermissionsResponse
      ) =>
        Number(a.canAccessLeadershipDashboard) -
        Number(b.canAccessLeadershipDashboard),
      render: (text, record) => {
        if (text != null) {
          return formatText(text.toString(), record);
        }
      },
    },
    {
      title: "Should See Beta Charts",
      dataIndex: "shouldSeeBetaCharts",
      key: "shouldSeeBetaCharts",
      width: 180,
      filters: [
        {
          value: true,
          text: "true",
        },
        {
          value: false,
          text: "false",
        },
      ],
      onFilter: (
        value: boolean | string | number,
        record: StateUserPermissionsResponse
      ) => {
        return record.shouldSeeBetaCharts === value;
      },
      sorter: (
        a: StateUserPermissionsResponse,
        b: StateUserPermissionsResponse
      ) => Number(a.shouldSeeBetaCharts) - Number(b.shouldSeeBetaCharts),
      render: (text, record) => {
        if (text != null) {
          return formatText(text.toString(), record);
        }
      },
    },
    {
      title: "Allowed Supervision Location IDs",
      dataIndex: "allowedSupervisionLocationIds",
      key: "allowedSupervisionLocationIds",
      filters: [...filterData(data)((d) => d.allowedSupervisionLocationIds)],
      onFilter: (
        value: string | number | boolean,
        record: StateUserPermissionsResponse
      ) => {
        return (
          record.allowedSupervisionLocationIds?.indexOf(
            value as keyof StateUserPermissionsResponse
          ) === 0
        );
      },
      sorter: (
        a: StateUserPermissionsResponse,
        b: StateUserPermissionsResponse
      ) =>
        a.allowedSupervisionLocationIds?.localeCompare(
          b.allowedSupervisionLocationIds
        ),
      render: (text, record) => {
        return formatText(text, record);
      },
    },
    {
      title: "Allowed Supervision Location Level",
      dataIndex: "allowedSupervisionLocationLevel",
      key: "allowedSupervisionLocationLevel",
      width: 220,
      filters: [...filterData(data)((d) => d.allowedSupervisionLocationLevel)],
      onFilter: (
        value: string | number | boolean,
        record: StateUserPermissionsResponse
      ) => {
        return (
          record.allowedSupervisionLocationLevel?.indexOf(
            value as keyof StateUserPermissionsResponse
          ) === 0
        );
      },
      sorter: (
        a: StateUserPermissionsResponse,
        b: StateUserPermissionsResponse
      ) =>
        a.allowedSupervisionLocationIds?.localeCompare(
          b.allowedSupervisionLocationIds
        ),
      render: (text, record) => {
        return formatText(text, record);
      },
    },
    {
      title: "Blocked",
      dataIndex: "blocked",
      key: "blocked",
      width: 150,
      filters: [
        {
          value: true,
          text: "true",
        },
        {
          value: false,
          text: "false",
        },
      ],
      onFilter: (
        value: boolean | string | number,
        record: StateUserPermissionsResponse
      ) => {
        return record.blocked === value;
      },
      sorter: (
        a: StateUserPermissionsResponse,
        b: StateUserPermissionsResponse
      ) => Number(a.blocked) - Number(b.blocked),
      render: (isBlocked, record) => {
        if (!isBlocked) {
          return undefined;
        }
        return (
          setUserToEnable && (
            <Button
              onClick={() => {
                setUserToEnable(record);
              }}
            >
              Enable user
            </Button>
          )
        );
      },
    },
  ];
};
