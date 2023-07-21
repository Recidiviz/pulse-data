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
import { Button, message, PageHeader, Space, Spin, Table } from "antd";
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
import { getStateRoleDefaultPermissions } from "../../AdminPanelAPI/LineStaffTools";
import { useFetchedDataJSON } from "../../hooks";
import {
  AddUserRequest,
  StateRolePermissionsResponse,
  StateUserForm,
  StateUserPermissionsResponse,
} from "../../types";
import { AddUserForm } from "./AddUserForm";
import { EditUserForm } from "./EditUsersForm";
import { CreateEnableUserForm } from "./EnableUserForm";
import { ReasonsLogButton } from "./ReasonsLogButton";
import { UploadStateUserRosterModal } from "./UploadStateUserRosterModal";
import {
  aggregateFormPermissionResults,
  checkResponse,
  getPermissionsTableColumns,
  updatePermissionsObject,
} from "./utils";

const StateUserPermissionsView = (): JSX.Element => {
  const { loading, data, setData } = useFetchedDataJSON<
    StateUserPermissionsResponse[]
  >(getStateUserPermissions);
  const { loading: roleLoading, data: stateRoleData } = useFetchedDataJSON<
    StateRolePermissionsResponse[]
  >(getStateRoleDefaultPermissions);
  // control modal visibility
  const [addVisible, setAddVisible] = useState(false);
  const [editVisible, setEditVisible] = useState(false);
  const [uploadRosterVisible, setUploadRosterVisible] = useState(false);
  const [tableKey, setTableKey] = useState(0);
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

  if (loading || roleLoading || !data || !stateRoleData) {
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
      userHash: userToEnable.userHash,
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
    reason,
    ...rest
  }: StateUserForm) => {
    const results: Promise<unknown>[] = [];
    selectedRows.forEach((row: StateUserPermissionsResponse) => {
      const editRow = async () => {
        // update user info
        if (role || district || externalId || firstName || lastName) {
          const updatedUser = await updateUser({
            userHash: row.userHash,
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
            row.userHash,
            reason
          );
          await checkResponse(deletedPermissions);
        }

        const { routes, featureVariantsToAdd, featureVariantsToRemove } =
          aggregateFormPermissionResults(rest);

        // update user's custom permissions
        const newRoutes = updatePermissionsObject(row.routes, routes, []);
        const newFeatureVariants = updatePermissionsObject(
          row.featureVariants,
          featureVariantsToAdd,
          featureVariantsToRemove
        );
        if (useCustomPermissions) {
          const updatedPermissions = await updateUserPermissions(
            row.userHash,
            reason,
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
      results.push(blockUser(row.userHash, reason));
    });
    finishPromises(results, `Blocked`);
  };

  const resetFilters = () => {
    setTableKey(() => tableKey + 1);
  };

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
        <AddUserForm
          addVisible={addVisible}
          addOnCreate={onAdd}
          addOnCancel={() => {
            setAddVisible(false);
          }}
          stateRoleData={stateRoleData}
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
        <ReasonsLogButton />
        <Button onClick={() => resetFilters()}>Reset Filters</Button>
        <EditUserForm
          editVisible={editVisible}
          editOnCreate={onEdit}
          editOnCancel={() => {
            setEditVisible(false);
          }}
          onRevokeAccess={onRevokeAccess}
          selectedUsers={selectedRows}
          stateRoleData={stateRoleData}
        />
        <UploadStateUserRosterModal
          visible={uploadRosterVisible}
          onCancel={() => {
            updateTable();
            setUploadRosterVisible(false);
          }}
          stateRoleData={stateRoleData}
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
        columns={getPermissionsTableColumns(
          data,
          stateRoleData,
          setUserToEnable
        )}
        scroll={{ x: 2000 }}
        key={tableKey}
        pagination={{ showSizeChanger: true }}
      />
    </>
  );
};

export default StateUserPermissionsView;
