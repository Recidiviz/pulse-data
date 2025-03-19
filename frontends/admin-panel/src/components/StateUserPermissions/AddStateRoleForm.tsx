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
import { Form, Input } from "antd";

import { StateRoleForm } from "../../types";
import { DraggableModal } from "../Utilities/DraggableModal";
import CustomPermissionsPanel from "./CustomPermissionsPanel";
import ReasonInput from "./ReasonInput";

export const CreateAddStateRoleForm = ({
  addVisible,
  addOnCreate,
  addOnCancel,
}: {
  addVisible: boolean;
  addOnCreate: (arg0: StateRoleForm) => Promise<void>;
  addOnCancel: () => void;
}): JSX.Element => {
  const [form] = Form.useForm();
  const role = Form.useWatch("role", form);
  const hidePermissions = role?.toLowerCase() === "unknown";

  return (
    <DraggableModal
      visible={addVisible}
      title="Add default permissions for a state/role"
      onCancel={addOnCancel}
      onOk={() => {
        form.validateFields().then((values) => {
          form.resetFields();
          addOnCreate(values);
        });
      }}
      width={700}
    >
      <Form form={form} layout="horizontal" labelCol={{ span: 8 }}>
        <ReasonInput label="Reason for addition" />
        <hr />
        <Form.Item
          name="stateCode"
          label="State Code"
          rules={[
            {
              required: true,
              pattern: /^US_[A-Z]{2}$/,
              message: "State Code is required and must have the form US_XX",
            },
          ]}
        >
          <Input />
        </Form.Item>
        <Form.Item
          name="role"
          label="Role"
          extra={
            hidePermissions
              ? "The 'unknown' role can be added but cannot be assigned permissions."
              : "Please try to use the same role names across states when possible."
          }
          rules={[
            {
              required: true,
              message: "Role is required. ex: leadership_user",
            },
          ]}
        >
          <Input />
        </Form.Item>
        <CustomPermissionsPanel hidePermissions={hidePermissions} form={form} />
      </Form>
    </DraggableModal>
  );
};
