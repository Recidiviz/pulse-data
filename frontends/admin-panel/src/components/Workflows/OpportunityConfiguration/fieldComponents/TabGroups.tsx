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

import { MultiEntry, MultiEntryChild } from "../../formUtils/MultiEntry";
import { StaticValue } from "../../formUtils/StaticValue";

const TabsView: MultiEntryChild = ({ name }) => (
  <Form.Item name={name} noStyle>
    <StaticValue />
  </Form.Item>
);

export const TabGroupsView: MultiEntryChild = ({ name }) => (
  <>
    <Form.Item noStyle name={[name, "key"]}>
      <StaticValue />
    </Form.Item>
    <MultiEntry label="Tabs" name={[name, "tabs"]} child={TabsView} readonly />
  </>
);

const TabsEdit: MultiEntryChild = ({ name }) => (
  <Form.Item
    noStyle
    name={name}
    rules={[{ required: true, message: "'title' is required" }]}
  >
    <Input placeholder="Title" />
  </Form.Item>
);

export const TabGroupsEdit: MultiEntryChild = ({ name }) => (
  <>
    <Form.Item
      noStyle
      name={[name, "key"]}
      rules={[{ required: true, message: "'group' is required" }]}
    >
      <Input placeholder="Group" />
    </Form.Item>
    <MultiEntry label="Tab Groups" name={[name, "tabs"]} child={TabsEdit} />
  </>
);
