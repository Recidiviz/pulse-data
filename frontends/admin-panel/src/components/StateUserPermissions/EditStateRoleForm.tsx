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
import { Form, Alert } from "antd";
import CustomPermissionsPanel from "./CustomPermissionsPanel";
import { DraggableModal } from "../Utilities/DraggableModal";

export const CreateEditStateRoleForm = ({
  editVisible,
  editOnCreate,
  editOnCancel,
  selectedRows,
}: {
  editVisible: boolean;
  editOnCreate: (arg0: StateRolePermissionsResponse) => Promise<void>;
  editOnCancel: () => void;
  selectedRows: StateRolePermissionsResponse[];
}): JSX.Element => {
  const [form] = Form.useForm();

  const selectedRowNames = selectedRows.map(
    (value) => `${value.stateCode}: ${value.role}`
  );

  return (
    <DraggableModal
      visible={editVisible}
      title="Update default permissions for a state/role"
      okText="Update"
      onCancel={editOnCancel}
      onOk={() => {
        form.validateFields().then((values) => {
          form.resetFields();
          editOnCreate(values);
        });
      }}
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
          {selectedRowNames.length} selected state/role(s):
        </span>
        <span style={{ fontStyle: "italic" }}>
          {" "}
          {selectedRowNames.join(", ")}
        </span>
      </p>
      <p>If any value is unset, the entry will not be changed.</p>
      <Form form={form} layout="horizontal" labelCol={{ span: 6 }}>
        <CustomPermissionsPanel hidePermissions={false} />
      </Form>
    </DraggableModal>
  );
};
