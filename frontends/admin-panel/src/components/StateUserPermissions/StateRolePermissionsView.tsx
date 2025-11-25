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
import React, { useState } from "react";

import {
  createStateRolePermissions,
  deleteFeatureVariant,
  deleteStateRole,
  getStateRoleDefaultPermissions,
  updateStateRolePermissions,
} from "../../AdminPanelAPI/LineStaffTools";
import { useFetchedDataJSON } from "../../hooks";
import {
  RemoveFeatureVariantForm,
  StateRoleForm,
  StateRolePermissionsResponse,
} from "../../types";
import { CreateAddStateRoleForm } from "./AddStateRoleForm";
import { CreateEditStateRoleForm } from "./EditStateRoleForm";
import { ReasonsLogButton } from "./ReasonsLogButton";
import { RemoveFeatureVariantsForm } from "./RemoveFeatureVariantsForm";
import {
  aggregateFormPermissionResults,
  checkResponse,
  getRolePermissionsTableColumns,
  updatePermissionsObject,
} from "./utils";

const StateRoleDefaultPermissionsView = (): JSX.Element => {
  const { loading, data, setData } = useFetchedDataJSON<
    StateRolePermissionsResponse[]
  >(getStateRoleDefaultPermissions);

  const [tableKey, setTableKey] = useState(0);

  // control modal visibility
  const [addVisible, setAddVisible] = useState(false);
  const [editVisible, setEditVisible] = useState(false);
  const [removeFvVisible, setRemoveFvVisible] = useState(false);

  // control row selection
  const [selectedRowKeys, setSelectedRowKeys] = useState<React.Key[]>([]);
  const [selectedRows, setSelectedRows] = useState<
    StateRolePermissionsResponse[]
  >([]);
  const onSelectChange = (
    newSelectedRowKeys: React.Key[],
    newSelectedRows: StateRolePermissionsResponse[]
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
    const stateRolePermissions = await getStateRoleDefaultPermissions();
    const stateRoleData = await stateRolePermissions.json();
    setData(stateRoleData);
  };

  const stateCodes = new Set<string>();
  data.forEach((row) => stateCodes.add(row.stateCode));
  const stateCodeFilters: { text: string; value: string }[] = [];
  stateCodes.forEach((stateCode: string) => {
    stateCodeFilters.push({
      text: stateCode,
      value: stateCode,
    });
  });

  const finishPromises = async (promises: Promise<unknown>[], verb: string) => {
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

  const onAdd = async ({ stateCode, role, reason, ...rest }: StateRoleForm) => {
    try {
      const {
        allowedApps,
        routes,
        featureVariantsToAdd,
        featureVariantsToRemove,
      } = aggregateFormPermissionResults(rest);
      const createdRole = await createStateRolePermissions(
        stateCode,
        role,
        reason,
        updatePermissionsObject({}, allowedApps, []),
        updatePermissionsObject({}, routes, []),
        updatePermissionsObject(
          {},
          featureVariantsToAdd,
          featureVariantsToRemove
        )
      );
      await checkResponse(createdRole);
      setAddVisible(false);
      message.success(`${role} added for ${stateCode}!`);
      updateTable();
    } catch (err) {
      message.error(`Error adding ${role} for ${stateCode}: ${err}`);
    }
  };

  const onEdit = async ({
    stateCode,
    role,
    reason,
    ...rest
  }: StateRoleForm) => {
    const results: Promise<unknown>[] = [];
    selectedRows.forEach((row: StateRolePermissionsResponse) => {
      const editRow = async () => {
        const {
          allowedApps,
          routes,
          featureVariantsToAdd,
          featureVariantsToRemove,
        } = aggregateFormPermissionResults(rest);
        const response = await updateStateRolePermissions(
          row.stateCode,
          row.role,
          reason,
          updatePermissionsObject({}, allowedApps, []),
          updatePermissionsObject({}, routes, []),
          updatePermissionsObject(
            {},
            featureVariantsToAdd,
            featureVariantsToRemove
          )
        );
        await checkResponse(response);
      };
      results.push(editRow());
    });
    finishPromises(results, `Updated`);
  };

  const onDelete = async (reason: string) => {
    const results: Promise<unknown>[] = [];
    selectedRows.forEach((row) => {
      const deleteRow = async () => {
        const response = await deleteStateRole(row.stateCode, row.role, reason);
        await checkResponse(response);
      };
      results.push(deleteRow());
    });
    finishPromises(results, `Removed`);
  };

  const onRemoveFv = async ({ fvName, reason }: RemoveFeatureVariantForm) => {
    try {
      const response = await deleteFeatureVariant({
        fvName,
        reason,
        entityType: "ROLES",
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

  const columns = getRolePermissionsTableColumns(data);

  return (
    <>
      <PageHeader title="State Role Default Permissions" />
      <Space>
        <Button
          onClick={() => {
            setAddVisible(true);
          }}
        >
          Add Permissions
        </Button>
        <CreateAddStateRoleForm
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
          Update Permissions
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
        <CreateEditStateRoleForm
          editVisible={editVisible}
          editOnCreate={onEdit}
          editOnCancel={() => {
            setEditVisible(false);
          }}
          editOnDelete={onDelete}
          selectedRows={selectedRows}
        />
      </Space>
      <br /> <br />
      <Table
        rowKey={(row) => `${row.stateCode}/${row.role}`}
        rowSelection={rowSelection}
        dataSource={data}
        columns={columns}
        pagination={{ defaultPageSize: 20, showSizeChanger: true }}
        key={tableKey}
      />
    </>
  );
};

export default StateRoleDefaultPermissionsView;
