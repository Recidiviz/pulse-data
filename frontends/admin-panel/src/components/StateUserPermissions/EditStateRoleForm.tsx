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
import { Form, Button, Popconfirm, Input } from "antd";
import CustomPermissionsPanel from "./CustomPermissionsPanel";
import { DraggableModal } from "../Utilities/DraggableModal";

export const CreateEditStateRoleForm = ({
  editVisible,
  editOnCreate,
  editOnCancel,
  editOnDelete,
  selectedRows,
}: {
  editVisible: boolean;
  editOnCreate: (arg0: StateRolePermissionsRequest) => Promise<void>;
  editOnCancel: () => void;
  editOnDelete: (reason: string) => Promise<void>;
  selectedRows: StateRolePermissionsResponse[];
}): JSX.Element => {
  const [form] = Form.useForm();

  const selectedRowNames = selectedRows.map(
    (value) => `${value.stateCode}: ${value.role}`
  );

  const handleCancel = () => {
    form.resetFields();
    editOnCancel();
  };
  const handleEdit = () => {
    form
      .validateFields()
      .then((values) => {
        form.resetFields();
        editOnCreate(values);
      })
      .catch((errorInfo) => {
        // hypothetically setting `scrollToFirstError` on the form should do this (or at least
        // scroll so the error is visible), but it doesn't seem to, so instead put the cursor in the
        // input directly.
        document.getElementById(errorInfo.errorFields?.[0].name?.[0])?.focus();
      });
  };
  const handleDelete = () => {
    form
      .validateFields()
      .then((values) => {
        form.resetFields();
        editOnDelete(values.reason);
      })
      .catch((errorInfo) => {
        // hypothetically setting `scrollToFirstError` on the form should do this (or at least
        // scroll so the error is visible), but it doesn't seem to, so instead put the cursor in the
        // input directly.
        document.getElementById(errorInfo.errorFields?.[0].name?.[0])?.focus();
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
        <Button type="primary" onClick={handleEdit} key="edit">
          Update
        </Button>,
      ]}
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
      <p>If any value is unset, the entry will not be changed.</p>
      <Form form={form} layout="horizontal" labelCol={{ span: 6 }}>
        <Form.Item
          name="reason"
          label="Reason for modification"
          labelCol={{ span: 9 }}
          rules={[
            {
              required: true,
              message: "Please input a reason for the change.",
            },
          ]}
        >
          <Input />
        </Form.Item>
        <hr />
        <CustomPermissionsPanel hidePermissions={false} />
      </Form>
    </DraggableModal>
  );
};
