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
import { Button, Form, Popconfirm } from "antd";

import { StateRoleForm, StateRolePermissionsResponse } from "../../types";
import { DraggableModal } from "../Utilities/DraggableModal";
import CustomPermissionsPanel from "./CustomPermissionsPanel";
import ReasonInput from "./ReasonInput";
import { validateAndFocus } from "./utils";

export const CreateEditStateRoleForm = ({
  editVisible,
  editOnCreate,
  editOnCancel,
  editOnDelete,
  selectedRows,
}: {
  editVisible: boolean;
  editOnCreate: (arg0: StateRoleForm) => Promise<void>;
  editOnCancel: () => void;
  editOnDelete: (reason: string) => Promise<void>;
  selectedRows: StateRolePermissionsResponse[];
}): JSX.Element => {
  const [form] = Form.useForm();
  const hidePermissions = selectedRows.some(
    (row) => row.role?.toLowerCase() === "unknown"
  );

  const selectedRowNames = selectedRows.map(
    (value) => `${value.stateCode}: ${value.role}`
  );

  const handleCancel = () => {
    form.resetFields();
    editOnCancel();
  };
  const handleEdit = () => {
    validateAndFocus<StateRoleForm>(form, (values) => {
      form.resetFields();
      editOnCreate(values);
    });
  };
  const handleDelete = () => {
    validateAndFocus<StateRoleForm>(form, (values) => {
      form.resetFields();
      editOnDelete(values.reason);
    });
  };
  const confirm = () =>
    new Promise((resolve) => {
      resolve(null);
      handleDelete();
    });
  return (
    <DraggableModal
      visible={editVisible}
      title="Update default permissions for a state/role"
      onCancel={handleCancel}
      footer={[
        <Button onClick={handleCancel} key="cancel">
          Cancel
        </Button>,
        <Popconfirm
          title="Confirm remove role(s)"
          onConfirm={confirm}
          key="delete"
        >
          <Button type="primary" danger>
            Remove role
          </Button>
        </Popconfirm>,
        <Button
          type="primary"
          onClick={handleEdit}
          key="edit"
          disabled={hidePermissions}
        >
          Update
        </Button>,
      ]}
      width={700}
    >
      <p>
        <span style={{ fontWeight: "bold" }}>
          {selectedRowNames.length} selected state/role(s):
        </span>
        <span style={{ fontStyle: "italic" }}>
          {" "}
          {selectedRowNames.join(", ")}
        </span>
      </p>
      {hidePermissions ? (
        <p>The &quot;unknown&quot; role cannot be assigned permissions.</p>
      ) : undefined}
      <p>If any value is unset, the entry will not be changed.</p>
      <Form form={form} layout="horizontal" labelCol={{ span: 6 }}>
        <ReasonInput label="Reason for modification" />
        <hr />
        <CustomPermissionsPanel hidePermissions={hidePermissions} form={form} />
      </Form>
    </DraggableModal>
  );
};
