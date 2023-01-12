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
import { DraggableModal } from "../Utilities/DraggableModal";

export const CreateAddUserForm = ({
  addVisible,
  addOnCreate,
  addOnCancel,
}: {
  addVisible: boolean;
  addOnCreate: (arg0: AddUserRequest) => Promise<void>;
  addOnCancel: () => void;
}): JSX.Element => {
  const [form] = Form.useForm();

  return (
    <DraggableModal
      visible={addVisible}
      title="Add a New User"
      onCancel={addOnCancel}
      onOk={() => {
        form
          .validateFields()
          .then((values) => {
            form.resetFields();
            addOnCreate(values);
          })
          .catch((errorInfo) => {
            // hypothetically setting `scrollToFirstError` on the form should do this (or at least
            // scroll so the error is visible), but it doesn't seem to, so instead put the cursor in the
            // input directly.
            document
              .getElementById(errorInfo.errorFields?.[0].name?.[0])
              ?.focus();
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
        <Form.Item
          name="reason"
          label="Reason for addition"
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
          label="State"
          rules={[
            {
              required: true,
              message: "Please input the user's state code.",
            },
          ]}
        >
          <Input />
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
          <Input />
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
