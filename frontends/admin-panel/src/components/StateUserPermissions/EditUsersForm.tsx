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
import { Alert, Button, Form, Input, Popconfirm, Select } from "antd";
import * as React from "react";
import { useState } from "react";
import { DraggableModal } from "../Utilities/DraggableModal";
import CustomPermissionsPanel from "./CustomPermissionsPanel";

export const CreateEditUserForm = ({
  editVisible,
  editOnCreate,
  editOnCancel,
  onRevokeAccess,
  selectedEmails,
}: {
  editVisible: boolean;
  editOnCreate: (arg0: StateUserPermissionsResponse) => Promise<void>;
  editOnCancel: () => void;
  onRevokeAccess: () => Promise<void>;
  selectedEmails: React.Key[];
}): JSX.Element => {
  const { Option } = Select;

  const [form] = Form.useForm();

  // control useCustomPermissions
  const [hidePermissions, setHidePermissions] = useState(true);
  function showPermissions(shouldShow: boolean) {
    setHidePermissions(!shouldShow);
  }

  const handleCancel = () => {
    form.resetFields();
    showPermissions(false);
    editOnCancel();
  };
  const handleEdit = () => {
    form.validateFields().then((values) => {
      form.resetFields();
      showPermissions(false);
      editOnCreate(values);
    });
  };

  const handleRevokeAccess = () => {
    form.resetFields();
    showPermissions(false);
    onRevokeAccess();
  };

  const confirm = () =>
    new Promise((resolve) => {
      handleRevokeAccess();
      resolve(null);
    });

  return (
    <DraggableModal
      visible={editVisible}
      title="Edit Selected User(s)"
      onCancel={handleCancel}
      footer={[
        <Button onClick={handleCancel} key="cancel">
          Cancel
        </Button>,
        <Popconfirm
          title="Confirm revoke access for user(s)"
          onConfirm={confirm}
          key="revoke"
        >
          <Button type="primary" danger>
            Revoke access
          </Button>
        </Popconfirm>,
        <Button type="primary" onClick={handleEdit} key="edit">
          Edit
        </Button>,
      ]}
    >
      <Alert
        message="Caution!"
        description="This form should only be used by members of the Polaris team."
        type="warning"
        showIcon
      />
      <br />
      <p>
        <span style={{ fontWeight: "bold" }}>
          {selectedEmails.length} selected user(s):
        </span>
        <span style={{ fontStyle: "italic" }}>
          {" "}
          {selectedEmails.join(", ")}
        </span>
      </p>
      <Form form={form} layout="horizontal" onFinish={editOnCreate}>
        <Form.Item name="role" label="Role" labelCol={{ span: 5 }}>
          <Input />
        </Form.Item>
        <Form.Item name="externalId" label="External ID" labelCol={{ span: 5 }}>
          <Input />
        </Form.Item>
        <Form.Item name="district" label="District" labelCol={{ span: 5 }}>
          <Input />
        </Form.Item>
        <Form.Item name="firstName" label="First Name" labelCol={{ span: 5 }}>
          <Input />
        </Form.Item>
        <Form.Item name="lastName" label="Last Name" labelCol={{ span: 5 }}>
          <Input />
        </Form.Item>
        <hr />
        <Form.Item
          name="useCustomPermissions"
          label="Use custom permissions"
          labelAlign="left"
        >
          <Select onChange={showPermissions} allowClear>
            <Option value>Add custom permissions</Option>
            <Option value={false}>Delete custom permissions</Option>
          </Select>
        </Form.Item>
        <CustomPermissionsPanel hidePermissions={hidePermissions} />
      </Form>
    </DraggableModal>
  );
};
