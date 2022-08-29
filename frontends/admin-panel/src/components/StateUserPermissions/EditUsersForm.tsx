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
import { Form, Input, Modal, Select, Alert } from "antd";
import * as React from "react";
import { useState } from "react";
import Draggable from "react-draggable";
import { DraggableModal } from "./utils";

export const CreateEditUserForm = ({
  editVisible,
  editOnCreate,
  editOnCancel,
  selectedEmails,
}: {
  editVisible: boolean;
  editOnCreate: (arg0: StateUserPermissionsResponse) => Promise<void>;
  editOnCancel: () => void;
  selectedEmails: React.Key[];
}): JSX.Element => {
  const [form] = Form.useForm();
  const draggableModal = DraggableModal();
  const { Option } = Select;

  // control useCustomPermissions
  const [hidePermissions, setHidePermissions] = useState(true);
  function showPermissions(shouldShow: boolean) {
    setHidePermissions(!shouldShow);
  }

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
      okText="Edit"
      cancelText="Cancel"
      onCancel={() => {
        form.resetFields();
        showPermissions(false);
        editOnCancel();
      }}
      onOk={() => {
        form.validateFields().then((values) => {
          form.resetFields();
          showPermissions(false);
          editOnCreate(values);
        });
      }}
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
        <Form.Item
          name="useCustomPermissions"
          label="Use custom permissions"
          labelAlign="left"
        >
          <Select defaultValue={null} onChange={showPermissions} allowClear>
            <Option value>Add custom permissions</Option>
            <Option value={false}>Delete custom permissions</Option>
          </Select>
        </Form.Item>
      </Form>
      <Form
        form={form}
        layout="horizontal"
        onFinish={editOnCreate}
        disabled={hidePermissions}
      >
        <Form.Item
          name="canAccessLeadershipDashboard"
          label="Can access leadership dashboard"
          labelCol={{ span: 15 }}
        >
          <Select
            defaultValue={null}
            allowClear
            style={{
              width: 80,
            }}
          >
            <Option value>True</Option>
            <Option value={false}>False</Option>
          </Select>
        </Form.Item>
        <Form.Item
          name="canAccessCaseTriage"
          label="Can access case triage"
          labelCol={{ span: 15 }}
        >
          <Select
            defaultValue={null}
            allowClear
            style={{
              width: 80,
            }}
          >
            <Option value>True</Option>
            <Option value={false}>False</Option>
          </Select>
        </Form.Item>
        <Form.Item
          name="shouldSeeBetaCharts"
          label="Should see beta charts"
          labelCol={{ span: 15 }}
        >
          <Select
            defaultValue={null}
            allowClear
            style={{
              width: 80,
            }}
          >
            <Option value>True</Option>
            <Option value={false}>False</Option>
          </Select>
        </Form.Item>
        <Form.Item name="routes" label="Routes">
          {/* TODO(#14749): Change field type to select dropdown menu or checkboxes */}
          <Input.TextArea placeholder='Format: {"routeOne": boolean, "routeTwo": boolean, ...}' />
        </Form.Item>
      </Form>
    </Modal>
  );
};
