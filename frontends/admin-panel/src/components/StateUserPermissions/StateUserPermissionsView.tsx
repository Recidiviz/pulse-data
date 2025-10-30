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
import { Button, message, PageHeader, Space, Spin, Table, Tooltip } from "antd";
import { isPast } from "date-fns";
import * as React from "react";
import { useEffect, useState } from "react";

import {
  blockUser,
  createNewUser,
  deleteCustomUserPermissions,
  getStateUserPermissions,
  updateUser,
  updateUserPermissions,
} from "../../AdminPanelAPI";
import {
  deleteFeatureVariant,
  getStateRoleDefaultPermissions,
  updateUsers,
} from "../../AdminPanelAPI/LineStaffTools";
import { useFetchedDataJSON } from "../../hooks";
import {
  AddUserRequest,
  RemoveFeatureVariantForm,
  StateRolePermissionsResponse,
  StateUserForm,
  StateUserPermissionsResponse,
} from "../../types";
import { AddUserForm } from "./AddUserForm";
import { EditUserForm } from "./EditUsersForm";
import { CreateEnableUserForm } from "./EnableUserForm";
import { ReasonsLogButton } from "./ReasonsLogButton";
import { RemoveFeatureVariantsForm } from "./RemoveFeatureVariantsForm";
import { UploadStateUserRosterModal } from "./UploadStateUserRosterModal";
import {
  aggregateFormPermissionResults,
  checkResponse,
  getUserPermissionsTableColumns,
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
  const [removeFvVisible, setRemoveFvVisible] = useState(false);
  const [uploadRosterVisible, setUploadRosterVisible] = useState(false);
  const [tableKey, setTableKey] = useState(0);
  const [userToEnable, setUserToEnable] = useState<
    StateUserPermissionsResponse | undefined
  >();

  // This canScroll state is used to prevent a ResizeObserver loop error
  // A bug in rc-resize-observer is requiring us to do this as a workaround
  // https://github.com/ant-design/ant-design/issues/26621#issuecomment-1798004981
  const [canScroll, setCanScroll] = useState(false);
  useEffect(() => {
    if (!loading && !!data) {
      setCanScroll(true);
    }
  }, [data, loading]);

  // control row selection
  const [selectedRowKeys, setSelectedRowKeys] = useState<React.Key[]>([]);
  const [selectedRows, setSelectedRows] = useState<
    StateUserPermissionsResponse[]
  >([]);
  const [selectedStates, setSelectedStates] = useState<string[]>([]);
  const onSelectChange = (
    newSelectedRowKeys: React.Key[],
    newSelectedRows: StateUserPermissionsResponse[]
  ) => {
    setSelectedRowKeys(newSelectedRowKeys);
    setSelectedRows(newSelectedRows);
    const stateCodes = newSelectedRows
      .map((d) => d.stateCode)
      .filter((v, i, a) => a.indexOf(v) === i);
    setSelectedStates(stateCodes);
  };
  const rowSelection = {
    selectedRowKeys,
    onChange: onSelectChange,
    getCheckboxProps: (record: StateUserPermissionsResponse) => ({
      disabled: !!record.blockedOn && isPast(record.blockedOn),
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
      blockedOn: null,
    });
    finishPromises([checkResponse(updatedUser)], "Enabled");
    setUserToEnable(undefined);
  };

  const onEdit = async ({
    emailAddress,
    stateCode,
    externalId,
    roles,
    district,
    firstName,
    lastName,
    useCustomPermissions,
    reason,
    ...rest
  }: StateUserForm) => {
    const results: Promise<unknown>[] = [];

    // update user info as a bulk operation
    if (roles || district || externalId || firstName || lastName) {
      const updatedUsers = await updateUsers(
        selectedRows.map((row) => {
          return {
            userHash: row.userHash,
            stateCode: row.stateCode,
            externalId,
            roles,
            district,
            firstName,
            lastName,
            reason,
          };
        })
      );
      results.push(checkResponse(updatedUsers));
    }

    selectedRows.forEach((row: StateUserPermissionsResponse) => {
      const editRow = async () => {
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
        const newRoutes = updatePermissionsObject(row.routes ?? {}, routes, []);
        const newFeatureVariants = updatePermissionsObject(
          row.featureVariants ?? {},
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

  const onRemoveFv = async ({ fvName, reason }: RemoveFeatureVariantForm) => {
    try {
      const response = await deleteFeatureVariant({
        fvName,
        reason,
        entityType: "USERS",
      });
      await checkResponse(response);
      setRemoveFvVisible(false);
      message.success(`${fvName} removed successfully!`);
      updateTable();
    } catch (err) {
      message.error(`Error removing ${fvName}: ${err}`);
    }
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
          userData={data}
        />
        <Tooltip
          title={
            selectedStates.length > 1
              ? "If editing multiple users at the same time, they must be from the same state"
              : ""
          }
        >
          <Button
            disabled={selectedStates.length !== 1}
            onClick={() => {
              setEditVisible(true);
            }}
          >
            Edit User(s)
          </Button>
        </Tooltip>
        <Button
          onClick={() => {
            setUploadRosterVisible(true);
          }}
        >
          Upload Roster
        </Button>
        <Button
          onClick={() => {
            setRemoveFvVisible(true);
          }}
        >
          Remove Feature Variant
        </Button>
        <RemoveFeatureVariantsForm
          removeFvVisible={removeFvVisible}
          removeFvOnCancel={() => {
            setRemoveFvVisible(false);
          }}
          removeFvOnCreate={onRemoveFv}
        />
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
          userData={data}
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
        columns={getUserPermissionsTableColumns(
          data,
          stateRoleData,
          setUserToEnable
        )}
        scroll={canScroll ? { x: 2000 } : undefined}
        key={tableKey}
        pagination={{ showSizeChanger: true }}
      />
    </>
  );
};

export default StateUserPermissionsView;
