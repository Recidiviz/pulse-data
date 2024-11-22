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

import { MultiEntryChild } from "../../formUtils/MultiEntry";
import { StaticValue } from "../../formUtils/StaticValue";

export const NotificationsView: MultiEntryChild = ({ name }) => (
  <>
    <Form.Item noStyle name={[name, "title"]}>
      <StaticValue />
    </Form.Item>
    <Form.Item noStyle name={[name, "body"]}>
      <StaticValue />
    </Form.Item>
    <Form.Item noStyle name={[name, "cta"]}>
      <StaticValue />
    </Form.Item>
  </>
);

export const NotificationsEdit: MultiEntryChild = ({ name }) => (
  <>
    <Form.Item noStyle name={[name, "title"]} rules={[{ required: false }]}>
      <Input placeholder="Title (optional)" />
    </Form.Item>
    :
    <Form.Item
      noStyle
      name={[name, "body"]}
      rules={[{ required: true, message: "Notification body is required" }]}
    >
      <Input placeholder="Body" />
    </Form.Item>
    :
    <Form.Item noStyle name={[name, "cta"]}>
      <Input placeholder="CTA (optional)" />
    </Form.Item>
  </>
);
