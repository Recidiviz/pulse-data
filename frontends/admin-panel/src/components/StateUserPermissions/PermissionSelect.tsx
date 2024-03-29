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
import { Form, FormItemProps, Select } from "antd";

type Permission = {
  name: string;
  label: string;
};

export const PermissionSelect = ({
  permission,
  disabled,
  placeholder,
  dependencies,
  rules,
}: {
  permission: Permission;
  disabled: boolean;
  placeholder?: string;
  dependencies?: FormItemProps["dependencies"];
  rules?: FormItemProps["rules"];
}): JSX.Element => {
  const permissionOptions = [
    {
      value: true,
      label: "True",
    },
    {
      value: false,
      label: "False",
    },
  ];

  return (
    <Form.Item
      name={[permission.name]}
      label={permission.label}
      labelCol={{ span: 15 }}
      dependencies={dependencies}
      rules={rules}
    >
      <Select
        allowClear
        style={{
          width: 80,
        }}
        options={permissionOptions}
        disabled={disabled}
        placeholder={placeholder}
      />
    </Form.Item>
  );
};

export default PermissionSelect;
