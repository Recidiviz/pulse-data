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
import { Alert, Button, Form, Input, Modal, Select } from "antd";
import * as React from "react";
import { useState } from "react";
import Draggable from "react-draggable";
import CustomPermissionsPanel from "./CustomPermissionsPanel";
import { DraggableModal } from "./utils";

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
  const draggableModal = DraggableModal();

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

  return (
    <Modal
      visible={editVisible}
      title={
        <div
          style={{
            width: "100%",
            cursor: "move",
          }}
          onMouseOver={() => {
            if (draggableModal.disabled) {
              draggableModal.setDisabled(false);
            }
          }}
          onMouseOut={() => {
            draggableModal.setDisabled(true);
          }}
          onFocus={() => undefined}
          onBlur={() => undefined}
        >
          Edit Selected User(s)
        </div>
      }
      onCancel={handleCancel}
      footer={[
        <Button onClick={handleCancel} key="cancel">
          Cancel
        </Button>,
        <Button type="primary" danger onClick={handleRevokeAccess} key="revoke">
          Revoke access
        </Button>,
        <Button type="primary" onClick={handleEdit} key="edit">
          Edit
        </Button>,
      ]}
      modalRender={(modal) => (
        <Draggable
          disabled={draggableModal.disabled}
          bounds={draggableModal.bounds}
          onStart={(event, uiData) => draggableModal.onStart(event, uiData)}
        >
          <div ref={draggableModal.dragRef}>{modal}</div>
        </Draggable>
      )}
      bodyStyle={{ overflowY: "scroll", maxHeight: "calc(100vh - 200px)" }}
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
    </Modal>
  );
};
