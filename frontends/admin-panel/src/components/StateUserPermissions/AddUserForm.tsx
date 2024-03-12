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
import { Col, Form, Input, Row, Select } from "antd";
import { useState } from "react";

import { AddUserRequest, StateRolePermissionsResponse } from "../../types";
import { DraggableModal } from "../Utilities/DraggableModal";
import { layout } from "./constants";
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
  const { gutter, colSpan } = layout;

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
        layout="vertical"
        onFinish={addOnCreate}
        labelCol={{ span: colSpan }}
      >
        <Row gutter={gutter}>
          <Col span={colSpan}>
            <Form.Item name="firstName" label="First Name">
              <Input />
            </Form.Item>
          </Col>
          <Col span={colSpan}>
            <Form.Item name="lastName" label="Last Name">
              <Input />
            </Form.Item>
          </Col>
        </Row>
        <Row gutter={gutter}>
          <Col span={colSpan * 2}>
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
          </Col>
        </Row>
        <Row gutter={gutter}>
          <Col span={colSpan}>
            <Form.Item
              name="stateCode"
              label="Select a State"
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
          </Col>
          <Col span={colSpan}>
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
          </Col>
        </Row>
        <Row gutter={gutter}>
          <Col span={colSpan}>
            <Form.Item name="externalId" label="External ID">
              <Input />
            </Form.Item>
          </Col>
          <Col span={colSpan}>
            <Form.Item name="district" label="District">
              <Input />
            </Form.Item>
          </Col>
        </Row>
        <Row gutter={gutter}>
          <Col span={colSpan * 2}>
            <ReasonInput label="Reason for Addition" />
          </Col>
        </Row>
      </Form>
    </DraggableModal>
  );
};
