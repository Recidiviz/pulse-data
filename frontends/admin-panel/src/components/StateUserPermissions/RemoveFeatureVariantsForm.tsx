// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2025 Recidiviz, Inc.
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
import { useLocation } from "react-router-dom";

import { RemoveFeatureVariantForm } from "../../types";
import { DraggableModal } from "../Utilities/DraggableModal";
import ReasonInput from "./ReasonInput";
import { Note } from "./styles";
import { validateAndFocus } from "./utils";

export const RemoveFeatureVariantsForm = ({
  removeFvVisible,
  removeFvOnCancel,
  removeFvOnCreate,
}: {
  removeFvVisible: boolean;
  removeFvOnCancel: () => void;
  removeFvOnCreate: (arg0: RemoveFeatureVariantForm) => Promise<void>;
}): JSX.Element => {
  const [form] = Form.useForm();

  const location = useLocation();
  const isRolePage = location.pathname.includes(
    "state_role_default_permissions"
  );
  const { noun, infoText } = {
    noun: isRolePage ? "roles" : "users",
    infoText: isRolePage
      ? "This will remove a feature variant from all state roles. If you would like to remove a feature variant from all users' custom permissions, remove it on the State User Permissions page."
      : "This will remove a feature variant from all users' custom permissions. If you would like to remove a feature variant from all state roles, remove it on the State Role Default Permissions page.",
  };

  return (
    <DraggableModal
      visible={removeFvVisible}
      title={`Remove feature variant from all ${noun}`}
      onCancel={removeFvOnCancel}
      okText="Remove"
      onOk={() => {
        validateAndFocus<RemoveFeatureVariantForm>(form, (values) => {
          form.resetFields();
          removeFvOnCreate(values);
        });
      }}
      width={700}
    >
      <Form form={form} layout="horizontal" labelCol={{ span: 8 }}>
        <Note>{infoText}</Note>
        <ReasonInput label="Reason for removing" />
        <Form.Item
          name="fvName"
          label="Feature Variant to Remove"
          rules={[
            {
              required: true,
              message: "Feature variant is required.",
            },
          ]}
        >
          <Input />
        </Form.Item>
      </Form>
    </DraggableModal>
  );
};
