// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2024 Recidiviz, Inc.
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
import { AddConfigurationRequest } from "../../types";
import { DraggableModal } from "../Utilities/DraggableModal";

export const AddConfigForm = ({
  stateCode,
  addVisible,
  addOnCancel,
  addOnSubmit,
}: {
  stateCode: string;
  addVisible: boolean;
  addOnCancel: () => void;
  addOnSubmit: (arg0: AddConfigurationRequest, arg1: string) => Promise<void>;
}): JSX.Element => {
  const [form] = Form.useForm();
  return (
    <DraggableModal
      visible={addVisible}
      title="Create new version"
      onCancel={addOnCancel}
      onOk={() => {
        form
          .validateFields()
          .then((values: AddConfigurationRequest) => {
            form.resetFields();
            addOnSubmit(values, stateCode);
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
      <Form form={form}>
        <Form.Item name="featureVariant" label="Feature Variant">
          <Input />
        </Form.Item>
        <Form.Item
          name="supervisionOfficerLabel"
          label="Supervision Officer Label"
          rules={[
            {
              required: true,
              message: `Please input what ${stateCode} calls its supervision staff member, e.g. "officer"`,
            },
          ]}
        >
          <Input />
        </Form.Item>
        <Form.Item
          name="supervisionDistrictLabel"
          label="Supervision District Label"
          rules={[
            {
              required: true,
              message: `Please input what ${stateCode} calls a location-based group of offices, e.g. "district"`,
            },
          ]}
        >
          <Input />
        </Form.Item>
        <Form.Item
          name="supervisionUnitLabel"
          label="Supervision Unit Label"
          rules={[
            {
              required: true,
              message: `Please input what ${stateCode} calls a group of supervision officers, e.g. "unit"`,
            },
          ]}
        >
          <Input />
        </Form.Item>
        <Form.Item
          name="supervisionSupervisorLabel"
          label="Supervision Supervisor Label"
          rules={[
            {
              required: true,
              message: `Please input what ${stateCode} calls a supervisor, e.g. "supervisor"`,
            },
          ]}
        >
          <Input />
        </Form.Item>
        <Form.Item
          name="supervisionDistrictManagerLabel"
          label="Supervision District Manager Label"
          rules={[
            {
              required: true,
              message: `Please input what ${stateCode} calls someone who manages supervision supervisors, e.g. "district director"`,
            },
          ]}
        >
          <Input />
        </Form.Item>
        <Form.Item
          name="supervisionJiiLabel"
          label="Supervision JII Label"
          rules={[
            {
              required: true,
              message: `Please input what ${stateCode} calls a justice-impacted individual on supervision, e.g. "client"`,
            },
          ]}
        >
          <Input />
        </Form.Item>
        <Form.Item
          name="learnMoreUrl"
          label="Learn More URL"
          rules={[
            {
              required: true,
              message: `Please input the URL that methodology/FAQ links can be pointed to for ${stateCode}`,
            },
          ]}
        >
          <Input />
        </Form.Item>
      </Form>
    </DraggableModal>
  );
};
