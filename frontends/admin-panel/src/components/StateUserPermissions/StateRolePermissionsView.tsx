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

import { PageHeader, Spin, Table } from "antd";
import { getStateRoleDefaultPermissions } from "../../AdminPanelAPI/LineStaffTools";
import { useFetchedDataJSON } from "../../hooks";

const StateRoleDefaultPermissionsView = (): JSX.Element => {
  const { loading, data } = useFetchedDataJSON<StateRolePermissionsResponse[]>(
    getStateRoleDefaultPermissions
  );

  if (loading || !data) {
    return (
      <div className="center">
        <Spin size="large" />
      </div>
    );
  }

  const stateCodes = new Set<string>();
  data.forEach((row) => stateCodes.add(row.stateCode));
  const stateCodeFilters: { text: string; value: string }[] = [];
  stateCodes.forEach((stateCode: string) => {
    stateCodeFilters.push({
      text: stateCode,
      value: stateCode,
    });
  });

  const columns = [
    {
      title: "State",
      dataIndex: "stateCode",
      key: "stateCode",
      filters: stateCodeFilters,
      onFilter: (
        value: string | number | boolean,
        record: StateRolePermissionsResponse
      ) =>
        record.stateCode.indexOf(
          value as keyof StateRolePermissionsResponse
        ) === 0,
      sorter: (
        a: StateRolePermissionsResponse,
        b: StateRolePermissionsResponse
      ) => a.stateCode.localeCompare(b.stateCode),
    },
    {
      title: "Role",
      dataIndex: "role",
    },
    {
      title: "Can Access Leadership Dashboard",
      dataIndex: "canAccessLeadershipDashboard",
      render: (text?: boolean) => (!!text).toString(),
    },
    {
      title: "Can Access Case Triage",
      dataIndex: "canAccessCaseTriage",
      render: (text?: boolean) => (!!text).toString(),
    },
    {
      title: "Should See Beta Charts",
      dataIndex: "shouldSeeBetaCharts",
      render: (text?: boolean) => (!!text).toString(),
    },
    {
      title: "Routes",
      dataIndex: "routes",
      width: 300,
      render: (
        text: Record<string, unknown>,
        record: StateRolePermissionsResponse
      ) => {
        if (text) {
          return JSON.stringify(text, null, 2).slice(2, -2);
        }
      },
    },
  ];

  return (
    <>
      <PageHeader title="State Role Default Permissions" />
      <br /> <br />
      <Table
        rowKey={(row) => `${row.stateCode}/${row.role}`}
        dataSource={data}
        columns={columns}
      />
    </>
  );
};

export default StateRoleDefaultPermissionsView;
