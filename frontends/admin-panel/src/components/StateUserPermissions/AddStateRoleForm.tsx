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
import { Form, Input, Modal, Alert, Select } from "antd";
import Draggable from "react-draggable";
import CustomPermissionsPanel from "./CustomPermissionsPanel";
import { DraggableModal } from "./utils";

const ROLES = ["leadership_role", "line_staff_user", "level_1_access_role"];

export const CreateAddStateRoleForm = ({
  addVisible,
  addOnCreate,
  addOnCancel,
}: {
  addVisible: boolean;
  addOnCreate: (arg0: StateRolePermissionsResponse) => Promise<void>;
  addOnCancel: () => void;
}): JSX.Element => {
  const [form] = Form.useForm();
  const draggableModal = DraggableModal();

  const roleOptions = ROLES.map((role) => (
    <Select.Option key={role}>{role}</Select.Option>
  ));

  return (
    <Modal
      visible={addVisible}
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
          Add default permissions for a state/role
        </div>
      }
      okText="Add"
      cancelText="Cancel"
      onCancel={addOnCancel}
      onOk={() => {
        form.validateFields().then((values) => {
          form.resetFields();
          addOnCreate(values);
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
    >
      <Alert
        message="Caution!"
        description="This form should only be used by members of the Polaris team."
        type="warning"
        showIcon
      />
      <br />
      <Form
        form={form}
        layout="horizontal"
        labelCol={{ span: 6 }}
        initialValues={{ role: ROLES[0] }}
      >
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
          label="Role Name"
          rules={[
            {
              required: true,
              message: "Role name is required.",
            },
          ]}
        >
          <Select>{roleOptions}</Select>
        </Form.Item>
        <CustomPermissionsPanel hidePermissions={false} />
      </Form>
    </Modal>
  );
};
