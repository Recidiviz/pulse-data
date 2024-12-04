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
import { ListView } from "../../formUtils/sharedComponents";
import { StaticValue } from "../../formUtils/StaticValue";

export const TabTextListView: MultiEntryChild = ({ name }) => (
  <>
    <Form.Item noStyle name={[name, "tab"]}>
      <StaticValue />
    </Form.Item>
    <MultiEntry label="Text" name={[name, "texts"]} child={ListView} readonly />
  </>
);

const TextListEdit: MultiEntryChild = ({ name }) => (
  <Form.Item
    noStyle
    name={name}
    rules={[{ required: true, message: "'text' is required" }]}
  >
    <Input placeholder="Text" />
  </Form.Item>
);

export const TabTextListEdit: MultiEntryChild = ({ name }) => (
  <div style={{ width: "100%" }}>
    <Form.Item
      noStyle
      name={[name, "tab"]}
      rules={[{ required: true, message: "'tab' is required" }]}
    >
      <Input placeholder="Tab" />
    </Form.Item>
    <MultiEntry label="Text" name={[name, "texts"]} child={TextListEdit} />
  </div>
);
