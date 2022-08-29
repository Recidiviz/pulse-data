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
import { Button, Input, message, PageHeader, Space, Spin, Table } from "antd";
import { FilterDropdownProps } from "antd/lib/table/interface";
import { SearchOutlined } from "@ant-design/icons";
import * as React from "react";
import { useState } from "react";
import {
  createNewUser,
  deleteCustomUserPermissions,
  getStateUserPermissions,
  updateUser,
  updateUserPermissions,
} from "../../AdminPanelAPI";
import { useFetchedDataJSON } from "../../hooks";
import { CreateAddUserForm } from "./AddUserForm";
import { CreateEditUserForm } from "./EditUsersForm";

const StateUserPermissionsView = (): JSX.Element => {
  const { loading, data, setData } = useFetchedDataJSON<
    StateUserPermissionsResponse[]
  >(getStateUserPermissions);

  // control modal visibility
  const [addVisible, setAddVisible] = useState(false);
  const [editVisible, setEditVisible] = useState(false);

  // control row selection
  const [selectedRowKeys, setSelectedRowKeys] = useState<React.Key[]>([]);
  const [selectedRows, setSelectedRows] = useState<
    StateUserPermissionsResponse[]
  >([]);
  const onSelectChange = (
    newSelectedRowKeys: React.Key[],
    newSelectedRows: StateUserPermissionsResponse[]
  ) => {
    setSelectedRowKeys(newSelectedRowKeys);
    setSelectedRows(newSelectedRows);
  };
  const rowSelection = {
    selectedRowKeys,
    onChange: onSelectChange,
  };

  if (loading || !data) {
    return (
      <div className="center">
        <Spin size="large" />
      </div>
    );
  }

  const updateTable = async () => {
    const users = await getStateUserPermissions();
    const userData = await users.json();
    setData(userData);
  };

  const checkResponse = async (response: Response) => {
    if (!response.ok) {
      const error = await response.text();
      throw error;
    }
  };

  const onAdd = async ({
    emailAddress,
    stateCode,
    externalId,
    role,
    district,
    firstName,
    lastName,
  }: StateUserPermissionsResponse) => {
    try {
      const createdUser = await createNewUser(
        emailAddress,
        stateCode,
        externalId,
        role,
        district,
        firstName,
        lastName
      );
      await checkResponse(createdUser);
      setAddVisible(false);
      message.success(`${emailAddress} added!`);
      updateTable();
    } catch (err) {
      message.error(`Error adding ${emailAddress}: ${err}`);
    }
  };

  const onEdit = async ({
    emailAddress,
    stateCode,
    externalId,
    role,
    district,
    firstName,
    lastName,
    useCustomPermissions,
    canAccessLeadershipDashboard,
    canAccessCaseTriage,
    shouldSeeBetaCharts,
    routes,
  }: StateUserPermissionsResponse) => {
    const results = [];
    for (let index = 0; index < selectedRowKeys.length; index += 1) {
      const email = selectedRows[index].emailAddress;
      const editRow = async () => {
        // update user info
        if (role || district || externalId || firstName || lastName) {
          const state = selectedRows[index].stateCode;
          const updatedUser = await updateUser(
            email,
            state,
            externalId,
            role,
            district,
            firstName,
            lastName
          );
          await checkResponse(updatedUser);
        }
        // delete user's custom permissions
        if (useCustomPermissions === false) {
          const deletedPermissions = await deleteCustomUserPermissions(email);
          await checkResponse(deletedPermissions);
        }
        // update user's custom permissions
        if (
          canAccessLeadershipDashboard != null ||
          canAccessCaseTriage != null ||
          shouldSeeBetaCharts != null ||
          routes != null
        ) {
          let newRoutes = routes;
          if (routes) {
            newRoutes = JSON.parse(routes);
          }
          const updatedPermissions = await updateUserPermissions(
            email,
            canAccessLeadershipDashboard,
            canAccessCaseTriage,
            shouldSeeBetaCharts,
            newRoutes
          );
          await checkResponse(updatedPermissions);
        }
        return "Success!";
      };
      results.push(editRow());
    }
    Promise.all(results)
      .then(() => {
        setEditVisible(false);
        message.success(`${selectedRowKeys.join(", ")} updated successfully!`);
      })
      .catch((error) => {
        message.error(`${error}`);
      })
      .finally(() => {
        updateTable();
      });
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
      dataIndex: "emailAddress",
      key: "emailAddress",
      width: 250,
      sorter: (
        a: StateUserPermissionsResponse,
        b: StateUserPermissionsResponse
      ) => a.emailAddress.localeCompare(b.emailAddress),
      ...getColumnSearchProps("emailAddress"),
    },
    {
      title: "State",
      dataIndex: "stateCode",
      key: "stateCode",
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
      title: "First Name",
      dataIndex: "firstName",
    },
    {
      title: "Last Name",
      dataIndex: "lastName",
    },
    {
      title: "Can Access Leadership Dashboard",
      dataIndex: "canAccessLeadershipDashboard",
      render: (_: string, record: StateUserPermissionsResponse) => {
        if (record.canAccessLeadershipDashboard != null) {
          return record.canAccessLeadershipDashboard.toString();
        }
      },
    },
    {
      title: "Can Access Case Triage",
      dataIndex: "canAccessCaseTriage",
      render: (_: string, record: StateUserPermissionsResponse) => {
        if (record.canAccessCaseTriage != null) {
          return record.canAccessCaseTriage.toString();
        }
      },
    },
    {
      title: "Should See Beta Charts",
      dataIndex: "shouldSeeBetaCharts",
      render: (_: string, record: StateUserPermissionsResponse) => {
        if (record.shouldSeeBetaCharts != null) {
          return record.shouldSeeBetaCharts.toString();
        }
      },
    },
    {
      title: "Routes",
      dataIndex: "routes",
      width: 300,
      render: (_: string, record: StateUserPermissionsResponse) => {
        if (record.routes) {
          return JSON.stringify(record.routes, null, "\t").slice(2, -2);
        }
      },
    },
    {
      title: "Allowed Supervision Location IDs",
      dataIndex: "allowedSupervisionLocationIds",
    },
    {
      title: "Allowed Supervision Location Level",
      dataIndex: "allowedSupervisionLocationLevel",
      width: 220,
    },
    {
      title: "Blocked",
      dataIndex: "blocked",
      render: (_: string, record: StateUserPermissionsResponse) => {
        if (record.blocked != null) {
          return record.blocked.toString();
        }
      },
    },
  ];

  return (
    <>
      <PageHeader title="State User Permissions" />
      <Space>
        <Button
          onClick={() => {
            setAddVisible(true);
          }}
        >
          Add User
        </Button>
        <CreateAddUserForm
          addVisible={addVisible}
          addOnCreate={onAdd}
          addOnCancel={() => {
            setAddVisible(false);
          }}
        />
        <Button
          onClick={() => {
            setEditVisible(true);
          }}
        >
          Edit User(s)
        </Button>
        <CreateEditUserForm
          editVisible={editVisible}
          editOnCreate={onEdit}
          editOnCancel={() => {
            setEditVisible(false);
          }}
          selectedEmails={selectedRowKeys}
        />
      </Space>
      <br /> <br />
      <Table
        rowKey="emailAddress"
        rowSelection={rowSelection}
        dataSource={data}
        columns={columns}
        scroll={{ x: 1700, y: 700 }}
      />
    </>
  );
};

export default StateUserPermissionsView;
