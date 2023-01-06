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

import { Button, message, PageHeader, Space, Spin, Table } from "antd";
import { useState } from "react";
import {
  createStateRolePermissions,
  deleteStateRole,
  getStateRoleDefaultPermissions,
  updateStateRolePermissions,
} from "../../AdminPanelAPI/LineStaffTools";
import { useFetchedDataJSON } from "../../hooks";
import { CreateAddStateRoleForm } from "./AddStateRoleForm";
import { ROUTES_PERMISSIONS_LABELS } from "../constants";
import { CreateEditStateRoleForm } from "./EditStateRoleForm";
import { checkResponse, updatePermissionsObject } from "./utils";

const StateRoleDefaultPermissionsView = (): JSX.Element => {
  const { loading, data, setData } = useFetchedDataJSON<
    StateRolePermissionsResponse[]
  >(getStateRoleDefaultPermissions);

  // control modal visibility
  const [addVisible, setAddVisible] = useState(false);
  const [editVisible, setEditVisible] = useState(false);

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

  const onAdd = async ({
    stateCode,
    role,
    canAccessLeadershipDashboard,
    canAccessCaseTriage,
    shouldSeeBetaCharts,
    ...routes
  }: StateRolePermissionsResponse) => {
    try {
      const createdRole = await createStateRolePermissions(
        stateCode,
        role,
        canAccessLeadershipDashboard,
        canAccessCaseTriage,
        shouldSeeBetaCharts,
        updatePermissionsObject({}, routes, ROUTES_PERMISSIONS_LABELS)
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
    canAccessLeadershipDashboard,
    canAccessCaseTriage,
    shouldSeeBetaCharts,
    ...routes
  }: StateRolePermissionsResponse) => {
    const results: Promise<unknown>[] = [];
    selectedRows.forEach((row: StateRolePermissionsResponse) => {
      const editRow = async () => {
        const response = await updateStateRolePermissions(
          row.stateCode,
          row.role,
          canAccessLeadershipDashboard,
          canAccessCaseTriage,
          shouldSeeBetaCharts,
          updatePermissionsObject({}, routes, ROUTES_PERMISSIONS_LABELS)
        );
        await checkResponse(response);
      };
      results.push(editRow());
    });
    finishPromises(results, `Updated`);
  };

  const onDelete = async () => {
    const results: Promise<unknown>[] = [];
    selectedRows.forEach((row) => {
      const deleteRow = async () => {
        const response = await deleteStateRole(row.stateCode, row.role);
        await checkResponse(response);
      };
      results.push(deleteRow());
    });
    finishPromises(results, `Removed`);
  };

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
          onClick={() => {
            setEditVisible(true);
          }}
        >
          Update Permissions
        </Button>
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
        pagination={{ defaultPageSize: 20 }}
      />
    </>
  );
};

export default StateRoleDefaultPermissionsView;
