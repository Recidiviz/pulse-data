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
import { Form, Input, Select } from "antd";
import { useState } from "react";
import { DraggableModal } from "../Utilities/DraggableModal";
import ReasonInput from "./ReasonInput";
import { validateAndFocus } from "./utils";

export const AddUserForm = ({
  addVisible,
  addOnCreate,
  addOnCancel,
  stateRoleData,
}: {
  addVisible: boolean;
  addOnCreate: (arg0: AddUserRequest) => Promise<void>;
  addOnCancel: () => void;
  stateRoleData: StateRolePermissionsResponse[];
}): JSX.Element => {
  const [form] = Form.useForm();
  const [roles, setRoles] = useState([] as string[]);

  const handleSelectStateCode = (stateCode: string) => {
    const permissionsForState = stateRoleData?.filter(
      (d) => d.stateCode === stateCode
    );
    const rolesForState = permissionsForState?.map((p) => p.role);
    if (rolesForState) setRoles(rolesForState);
  };

  const stateCodes = stateRoleData
    .map((d) => d.stateCode)
    .filter((v, i, a) => a.indexOf(v) === i);

  return (
    <DraggableModal
      visible={addVisible}
      title="Add a New User"
      onCancel={addOnCancel}
      onOk={() => {
        validateAndFocus<AddUserRequest>(form, (values) => {
          form.resetFields();
          addOnCreate(values);
        });
      }}
    >
      <br />
      <Form
        form={form}
        layout="horizontal"
        onFinish={addOnCreate}
        labelCol={{ span: 8 }}
      >
        <ReasonInput label="Reason for addition" />
        <hr />
        <Form.Item
          name="emailAddress"
          label="Email Address"
          rules={[
            {
              required: true,
              message: "Please input the user's email address.",
            },
            { type: "email", message: "Please enter a valid email." },
          ]}
        >
          <Input />
        </Form.Item>
        <Form.Item
          name="stateCode"
          label="State code"
          rules={[
            {
              required: true,
              message: "Please input the user's state code.",
            },
          ]}
        >
          <Select
            onChange={handleSelectStateCode}
            options={stateCodes.map((c) => {
              return { value: c, label: c };
            })}
          />
        </Form.Item>
        <Form.Item
          name="role"
          label="Role"
          rules={[
            {
              required: true,
              message: "Please input the user's role.",
            },
          ]}
        >
          <Select
            options={roles.map((r) => {
              return { value: r, label: r };
            })}
            disabled={roles.length === 0}
          />
        </Form.Item>
        <Form.Item name="externalId" label="External ID">
          <Input />
        </Form.Item>
        <Form.Item name="district" label="District">
          <Input />
        </Form.Item>
        <Form.Item name="firstName" label="First Name">
          <Input />
        </Form.Item>
        <Form.Item name="lastName" label="Last Name">
          <Input />
        </Form.Item>
      </Form>
    </DraggableModal>
  );
};
