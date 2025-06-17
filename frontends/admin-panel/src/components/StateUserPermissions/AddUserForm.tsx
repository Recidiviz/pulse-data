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

import {
  AddUserRequest,
  StateRolePermissionsResponse,
  StateUserPermissionsResponse,
} from "../../types";
import { STATE_CODES_TO_NAMES } from "../constants";
import { DraggableModal } from "../Utilities/DraggableModal";
import { layout } from "./constants";
import CustomInputSelect from "./CustomInputSelect";
import ReasonInput from "./ReasonInput";
import { getStateDistricts, getStateRoles, validateAndFocus } from "./utils";

export const AddUserForm = ({
  addVisible,
  addOnCreate,
  addOnCancel,
  stateRoleData,
  userData,
}: {
  addVisible: boolean;
  addOnCreate: (arg0: AddUserRequest) => Promise<void>;
  addOnCancel: () => void;
  stateRoleData: StateRolePermissionsResponse[];
  userData: StateUserPermissionsResponse[];
}): JSX.Element => {
  const [form] = Form.useForm();
  const [stateSelected, setStateSelected] = useState(false);
  const [roles, setRoles] = useState([] as string[]);
  const [districts, setDistricts] = useState([] as string[]);
  const { gutter, colSpan } = layout;

  const handleSelectStateCode = (stateCode: string) => {
    setStateSelected(true);
    const stateCodeForUser = [stateCode];

    // Determine roles available for state
    const rolesForState = getStateRoles(stateCodeForUser, stateRoleData);
    if (rolesForState) setRoles(rolesForState);

    // Determine districts available for state
    const districtsForState = getStateDistricts(stateCodeForUser, userData);
    if (districtsForState) setDistricts(districtsForState);
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
                  return {
                    value: c,
                    label:
                      STATE_CODES_TO_NAMES[
                        c as keyof typeof STATE_CODES_TO_NAMES
                      ] || c,
                  };
                })}
              />
            </Form.Item>
          </Col>
          <Col span={colSpan}>
            <Form.Item
              name="roles"
              label="Roles"
              rules={[
                {
                  required: true,
                  message: "Please input the user's role.",
                },
              ]}
            >
              <Select
                mode="multiple"
                options={roles.map((r) => {
                  return { value: r, label: r };
                })}
                disabled={!stateSelected}
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
              <Select
                // eslint-disable-next-line react/no-unstable-nested-components
                dropdownRender={(menu) => (
                  <CustomInputSelect
                    menu={menu}
                    items={districts}
                    setItems={setDistricts}
                    field="district"
                    setField={form.setFieldValue}
                  />
                )}
                options={districts
                  .map((d) => ({ value: d, label: d }))
                  .sort((a, b) => a.value.localeCompare(b.value))}
                disabled={!stateSelected}
                allowClear
              />
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
