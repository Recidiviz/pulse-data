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

import { Form, Input, Select } from "antd";

import { MultiEntryChild } from "../../formUtils/MultiEntry";
import { StaticValue } from "../../formUtils/StaticValue";

export const CompareByView: MultiEntryChild = ({ name }) => (
  <Form.Item noStyle name={[name, "field"]}>
    <StaticValue />
  </Form.Item>
);

export const CompareByEdit: MultiEntryChild = ({ name }) => (
  <div style={{ width: "100%" }}>
    <Form.Item
      noStyle
      name={[name, "field"]}
      rules={[{ required: true, message: "'field' is required" }]}
    >
      <Input placeholder="Field" />
    </Form.Item>
    <div style={{ marginTop: "0.25em", display: "flex", gap: "2em" }}>
      <Form.Item
        label="Direction"
        name={[name, "sortDirection"]}
        rules={[{ required: false }]}
        style={{ flexGrow: 1 }}
      >
        <Select>
          <Select.Option value="asc">Ascending</Select.Option>
          <Select.Option value="desc">Descending</Select.Option>
        </Select>
      </Form.Item>
      <Form.Item
        label="Missing Values"
        name={[name, "undefinedBehavior"]}
        rules={[{ required: false }]}
        style={{ flexGrow: 1 }}
      >
        <Select>
          <Select.Option value="undefinedFirst">First</Select.Option>
          <Select.Option value="undefinedLast">Last</Select.Option>
        </Select>
      </Form.Item>
    </div>
  </div>
);
