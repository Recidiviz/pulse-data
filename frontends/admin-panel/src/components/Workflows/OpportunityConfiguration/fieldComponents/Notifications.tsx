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

export const NotificationsView: MultiEntryChild = ({ name }) => (
  <>
    <Form.Item name={[name, "title"]} label="Title">
      <StaticValue />
    </Form.Item>
    <Form.Item
      name={[name, "pages"]}
      label="Pages"
      help="Pages where this notification will be displayed."
    >
      <StaticValue />
    </Form.Item>
    <Form.Item
      name={[name, "type"]}
      label="Type"
      help={`'Info (Blue)' for informational messages or 'Alert (Red)' for important alerts.`}
    >
      <StaticValue />
    </Form.Item>
    <Form.Item name={[name, "body"]} label="Body">
      <StaticValue />
    </Form.Item>
    <Form.Item name={[name, "cta"]} label="CTA">
      <StaticValue />
    </Form.Item>
  </>
);

export const NotificationsEdit: MultiEntryChild = ({ name }) => (
  <>
    <Form.Item
      name={[name, "title"]}
      label="Title"
      rules={[{ required: false }]}
    >
      <Input placeholder="Title (optional)" />
    </Form.Item>
    <Form.Item
      name={[name, "type"]}
      label="Type"
      help="Select the notification type: 'Info' for informational messages or 'Alert' for important alerts. Defaults to 'Info'."
      normalize={(value) => value || "info"}
    >
      <Select
        options={[
          { label: "Info (Blue)", value: "info" },
          { label: "Alert (Red)", value: "alert" },
        ]}
      />
    </Form.Item>
    <Form.Item
      name={[name, "pages"]}
      label="Pages"
      help="Select the pages where this notification will be displayed. You can select multiple pages. Pages marked with '{{ }}' support handlebar template variables. Defaults to 'Caseload' when no other pages are selected."
      normalize={(value) => (value && value.length > 0 ? value : ["caseload"])}
    >
      <Select
        mode="multiple"
        options={[
          { label: "Caseload", value: "caseload" },
          {
            label: "Profile {{ }}",
            value: "profile",
          },
          {
            label: "Supervision Supervisor {{ }}",
            value: "supervisionSupervisor",
          },
        ]}
      />
    </Form.Item>
    <Form.Item
      name={[name, "body"]}
      label="Body"
      rules={[{ required: true, message: "Notification body is required" }]}
    >
      <Input placeholder="Body" />
    </Form.Item>
    <Form.Item name={[name, "cta"]} label="CTA">
      <Input placeholder="CTA (optional)" />
    </Form.Item>
  </>
);
