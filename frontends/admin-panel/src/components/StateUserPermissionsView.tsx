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
import * as React from "react";
// import { getStateUserPermissions } from "../AdminPanelAPI";
import { useFetchedDataJSON } from "../hooks";

const StateUserPermissionsView = (): JSX.Element => {
  const dataSource = [
    {
      user: "User 1",
      permission1: "Yes",
      permission2: "Yes",
      permission3: "Yes",
      permission4: "Yes",
    },
    {
      user: "User 2",
      permission1: "No",
      permission2: "No",
      permission3: "No",
      permission4: "No",
    },
    {
      user: "User 3",
      permission1: "No",
      permission2: "No",
      permission3: "Yes",
      permission4: "Yes",
    },
  ];

  const columns = [
    {
      title: "User",
      key: "user",
      dataIndex: "user",
    },
    {
      title: "Permission 1",
      key: "permission1",
      dataIndex: "permission1",
    },
    {
      title: "Permission 2",
      key: "permission2",
      dataIndex: "permission2",
    },
    {
      title: "Permission 3",
      key: "permission3",
      dataIndex: "permission3",
    },
    {
      title: "Permission 4",
      key: "permission4",
      dataIndex: "permission4",
    },
  ];

  return (
    <>
      <PageHeader title="State User Permissions" />
      <Table dataSource={dataSource} columns={columns} />
    </>
  );
};

export default StateUserPermissionsView;
