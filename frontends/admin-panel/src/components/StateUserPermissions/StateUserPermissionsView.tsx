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
import { SearchOutlined } from "@ant-design/icons";
import {
  Button,
  Input,
  message,
  PageHeader,
  Space,
  Spin,
  Table,
  Typography,
} from "antd";
import { FilterDropdownProps } from "antd/lib/table/interface";
import * as React from "react";
import { useState } from "react";
import {
  blockUser,
  createNewUser,
  deleteCustomUserPermissions,
  getStateUserPermissions,
  updateUser,
  updateUserPermissions,
} from "../../AdminPanelAPI";
import { useFetchedDataJSON } from "../../hooks";
import { CreateAddUserForm } from "./AddUserForm";
import { CreateEditUserForm } from "./EditUsersForm";
import { UploadStateUserRosterModal } from "./UploadStateUserRosterModal";
import {
  FEATURE_VARIANTS_LABELS,
  ROUTES_PERMISSIONS_LABELS,
  USER_ROLES,
} from "../constants";
import { checkResponse, updatePermissionsObject } from "./utils";
import { CreateEnableUserForm } from "./EnableUserForm";

const StateUserPermissionsView = (): JSX.Element => {
  const { loading, data, setData } = useFetchedDataJSON<
    StateUserPermissionsResponse[]
  >(getStateUserPermissions);
  // control modal visibility
  const [addVisible, setAddVisible] = useState(false);
  const [editVisible, setEditVisible] = useState(false);
  const [uploadRosterVisible, setUploadRosterVisible] = useState(false);
  const [userToEnable, setUserToEnable] =
    useState<StateUserPermissionsResponse | undefined>();

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
    getCheckboxProps: (record: StateUserPermissionsResponse) => ({
      disabled: record.blocked === true,
    }),
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

  const finishPromises = async (
    promises: Array<Promise<unknown>>,
    verb: string
  ) => {
    if (promises.length !== 0) {
      Promise.all(promises)
        .then(() => {
          setEditVisible(false);
          message.success(
            `${verb} ${selectedRowKeys.join(", ")} successfully!`
          );
          setSelectedRowKeys([]); // clear selected rows once all changes are successful
        })
        .catch((error) => {
          message.error(`${error}`);
        })
        .finally(() => {
          updateTable();
        });
    }
  };

  const onAdd = async (request: AddUserRequest) => {
    try {
      const createdUser = await createNewUser(request);
      await checkResponse(createdUser);
      setAddVisible(false);
      message.success(`${request.emailAddress} added!`);
      updateTable();
    } catch (err) {
      message.error(`Error adding ${request.emailAddress}: ${err}`);
    }
  };

  const onEnableUser = async (reason: string) => {
    if (!userToEnable) {
      return;
    }
    const updatedUser = await updateUser({
      email: userToEnable.emailAddress,
      stateCode: userToEnable.stateCode,
      reason,
      blocked: false,
    });
    finishPromises([checkResponse(updatedUser)], "Enabled");
    setUserToEnable(undefined);
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
    reason,
    ...rest
  }: StateUserPermissionsRequest) => {
    const results: Promise<unknown>[] = [];

    selectedRows.forEach((row: StateUserPermissionsResponse) => {
      const editRow = async () => {
        // update user info
        if (role || district || externalId || firstName || lastName) {
          const updatedUser = await updateUser({
            email: row.emailAddress,
            stateCode: row.stateCode,
            externalId,
            role,
            district,
            firstName,
            lastName,
            reason,
          });
          await checkResponse(updatedUser);
        }

        // delete user's custom permissions
        if (useCustomPermissions === false) {
          const deletedPermissions = await deleteCustomUserPermissions(
            row.emailAddress,
            reason
          );
          await checkResponse(deletedPermissions);
        }

        // update user's custom permissions
        const newRoutes = updatePermissionsObject(
          row.routes,
          rest,
          ROUTES_PERMISSIONS_LABELS
        );
        const newFeatureVariants = updatePermissionsObject(
          row.featureVariants,
          rest,
          FEATURE_VARIANTS_LABELS
        );
        if (useCustomPermissions) {
          const updatedPermissions = await updateUserPermissions(
            row.emailAddress,
            reason,
            canAccessLeadershipDashboard,
            canAccessCaseTriage,
            shouldSeeBetaCharts,
            newRoutes,
            newFeatureVariants
          );
          await checkResponse(updatedPermissions);
        }
        return "Success!";
      };
      results.push(editRow());
    });
    finishPromises(results, `Updated`);
  };

  const onRevokeAccess = async (reason: string) => {
    const results: Array<Promise<Response>> = [];
    selectedRows.forEach((row) => {
      results.push(blockUser(row.emailAddress, reason));
    });
    finishPromises(results, `Blocked`);
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

  const { Text } = Typography;
  const formatText = (text: string, record: StateUserPermissionsResponse) => {
    if (record.blocked === true) {
      return (
        <Text type="secondary" italic>
          {text}
        </Text>
      );
    }
    return text;
  };
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
      render: (text: string, record: StateUserPermissionsResponse) => {
        return formatText(text, record);
      },
    },
    {
      title: "State",
      dataIndex: "stateCode",
      key: "stateCode",
      width: 100,
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
      render: (text: string, record: StateUserPermissionsResponse) => {
        return formatText(text, record);
      },
    },
    {
      title: "First Name",
      dataIndex: "firstName",
      width: 200,
      render: (text: string, record: StateUserPermissionsResponse) => {
        return formatText(text, record);
      },
    },
    {
      title: "Last Name",
      dataIndex: "lastName",
      width: 200,
      render: (text: string, record: StateUserPermissionsResponse) => {
        return formatText(text, record);
      },
    },
    {
      title: "Role",
      dataIndex: "role",
      width: 150,
      filters: USER_ROLES.map((roleName) => {
        return { text: roleName, value: roleName };
      }),
      onFilter: (
        value: string | number | boolean,
        record: StateUserPermissionsResponse
      ) =>
        record.role.indexOf(value as keyof StateUserPermissionsResponse) === 0,
      sorter: (
        a: StateUserPermissionsResponse,
        b: StateUserPermissionsResponse
      ) => a.role.localeCompare(b.role),
      render: (text: string, record: StateUserPermissionsResponse) => {
        return formatText(text, record);
      },
    },
    {
      title: "Routes",
      dataIndex: "routes",
      width: 300,
      render: (
        text: Record<string, unknown>,
        record: StateUserPermissionsResponse
      ) => {
        if (text) {
          return formatText(
            JSON.stringify(text, null, "\t").slice(2, -2),
            record
          );
        }
      },
    },
    {
      title: "Feature variants",
      dataIndex: "featureVariants",
      width: 350,
      render: (
        text: Record<string, unknown>,
        record: StateUserPermissionsResponse
      ) => {
        if (text) {
          return formatText(
            JSON.stringify(text, null, "\t").slice(2, -2),
            record
          );
        }
      },
    },
    {
      title: "Can Access Leadership Dashboard",
      dataIndex: "canAccessLeadershipDashboard",
      width: 150,
      render: (text: boolean, record: StateUserPermissionsResponse) => {
        if (text != null) {
          return formatText(text.toString(), record);
        }
      },
    },
    {
      title: "Can Access Case Triage",
      dataIndex: "canAccessCaseTriage",
      width: 150,
      render: (text: boolean, record: StateUserPermissionsResponse) => {
        if (text != null) {
          return formatText(text.toString(), record);
        }
      },
    },
    {
      title: "Should See Beta Charts",
      dataIndex: "shouldSeeBetaCharts",
      width: 150,
      render: (text: boolean, record: StateUserPermissionsResponse) => {
        if (text != null) {
          return formatText(text.toString(), record);
        }
      },
    },
    {
      title: "Allowed Supervision Location IDs",
      dataIndex: "allowedSupervisionLocationIds",
      render: (text: string, record: StateUserPermissionsResponse) => {
        return formatText(text, record);
      },
    },
    {
      title: "Allowed Supervision Location Level",
      dataIndex: "allowedSupervisionLocationLevel",
      width: 220,
      render: (text: string, record: StateUserPermissionsResponse) => {
        return formatText(text, record);
      },
    },
    {
      title: "Blocked",
      dataIndex: "blocked",
      width: 150,
      render: (isBlocked: boolean, record: StateUserPermissionsResponse) => {
        if (!isBlocked) {
          return undefined;
        }
        return (
          <Button
            onClick={() => {
              setUserToEnable(record);
            }}
          >
            Enable user
          </Button>
        );
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
          disabled={selectedRowKeys.length < 1}
          onClick={() => {
            setEditVisible(true);
          }}
        >
          Edit User(s)
        </Button>
        <Button
          onClick={() => {
            setUploadRosterVisible(true);
          }}
        >
          Upload Roster
        </Button>
        <CreateEditUserForm
          editVisible={editVisible}
          editOnCreate={onEdit}
          editOnCancel={() => {
            setEditVisible(false);
          }}
          onRevokeAccess={onRevokeAccess}
          selectedEmails={selectedRowKeys}
        />
        <UploadStateUserRosterModal
          visible={uploadRosterVisible}
          onCancel={() => {
            updateTable();
            setUploadRosterVisible(false);
          }}
        />
        <CreateEnableUserForm
          enableVisible={!!userToEnable}
          enableOnCreate={onEnableUser}
          enableOnCancel={() => {
            setUserToEnable(undefined);
          }}
        />
      </Space>
      <br /> <br />
      <Table
        rowKey="emailAddress"
        rowSelection={rowSelection}
        dataSource={data}
        columns={columns}
        scroll={{ x: 2000 }}
      />
    </>
  );
};

export default StateUserPermissionsView;
