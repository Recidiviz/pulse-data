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
import { Button, Form, Input, Popconfirm, Select } from "antd";
import { useState } from "react";
import {
  StateRolePermissionsResponse,
  StateUserForm,
  StateUserPermissionsResponse,
} from "../../types";
import { DraggableModal } from "../Utilities/DraggableModal";
import CustomPermissionsPanel from "./CustomPermissionsPanel";
import ReasonInput from "./ReasonInput";
import { validateAndFocus } from "./utils";
import { Note } from "./styles";

export const EditUserForm = ({
  editVisible,
  editOnCreate,
  editOnCancel,
  onRevokeAccess,
  selectedUsers,
  stateRoleData,
}: {
  editVisible: boolean;
  editOnCreate: (arg0: StateUserForm) => Promise<void>;
  editOnCancel: () => void;
  onRevokeAccess: (reason: string) => Promise<void>;
  selectedUsers: StateUserPermissionsResponse[];
  stateRoleData: StateRolePermissionsResponse[];
}): JSX.Element => {
  const { Option } = Select;
  const [form] = Form.useForm();

  const stateCodes = selectedUsers
    .map((u) => u.stateCode)
    .filter((v, i, a) => a.indexOf(v) === i);

  // Determine roles available for selected user(s)
  const permissionsForState = stateRoleData?.filter((d) =>
    stateCodes.includes(d.stateCode)
  );
  const rolesForState = permissionsForState?.map((p) => p.role);

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
    validateAndFocus<StateUserForm>(form, (values) => {
      form.resetFields();
      showPermissions(false);
      editOnCreate(values);
    });
  };

  const handleRevokeAccess = () => {
    validateAndFocus<StateUserForm>(form, (values) => {
      form.resetFields();
      showPermissions(false);
      onRevokeAccess(values.reason);
    });
  };

  const confirm = () =>
    new Promise((resolve) => {
      handleRevokeAccess();
      resolve(null);
    });

  const selectedEmails = selectedUsers.map((u) => u.emailAddress);

  const singleUserEdit = selectedEmails.length === 1;

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
      width={700}
    >
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
        <ReasonInput label="Reason for modification" />
        <hr />
        <Form.Item name="role" label="Role" labelCol={{ span: 5 }}>
          <Select
            options={rolesForState.map((r) => {
              return { value: r, label: r };
            })}
            disabled={rolesForState.length === 0 || stateCodes.length > 1}
            placeholder={singleUserEdit ? selectedUsers[0].role : undefined}
          />
        </Form.Item>
        <Form.Item name="externalId" label="External ID" labelCol={{ span: 5 }}>
          <Input disabled={!singleUserEdit} />
        </Form.Item>
        <Form.Item name="district" label="District" labelCol={{ span: 5 }}>
          <Input
            placeholder={selectedUsers.map((u) => u.district).join(", ")}
          />
        </Form.Item>
        <Form.Item name="firstName" label="First Name" labelCol={{ span: 5 }}>
          <Input
            disabled={!singleUserEdit}
            placeholder={
              singleUserEdit ? selectedUsers[0].firstName : undefined
            }
          />
        </Form.Item>
        <Form.Item name="lastName" label="Last Name" labelCol={{ span: 5 }}>
          <Input
            disabled={!singleUserEdit}
            placeholder={singleUserEdit ? selectedUsers[0].lastName : undefined}
          />
        </Form.Item>
        <hr />
        <Note>
          To set custom permissions or feature variants for a user, select
          &quot;Add custom permissions&quot; in the dropdown. To revert to the
          default for the user&apos;s state+role, select &quot;Delete custom
          permissions&quot;. Custom permissions and feature variants will be
          applied on top of the defaults, so if a default permission enables
          Workflows and a custom one enables Vitals, the user will have access
          to both Workflows and Vitals.
        </Note>
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
        <CustomPermissionsPanel
          hidePermissions={hidePermissions}
          form={form}
          selectedUsers={selectedUsers}
        />
      </Form>
    </DraggableModal>
  );
};
