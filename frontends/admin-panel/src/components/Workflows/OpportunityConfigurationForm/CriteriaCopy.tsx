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
import TextArea from "antd/lib/input/TextArea";

import { MultiEntry } from "./MultiEntry";

export const CriteriaCopy = ({
  name: outerName,
  label,
}: {
  name: string;
  label: string;
}) => (
  <MultiEntry label={label} name={outerName}>
    {({ name, ...field }) => (
      <div style={{ width: "100%", marginBottom: "0.25em" }}>
        <Form.Item
          {...field}
          noStyle
          name={[name, 0]}
          rules={[{ required: true, message: "'criteria' is required" }]}
        >
          <Input placeholder="Criteria" />
        </Form.Item>
        <div style={{ marginTop: "0.25em", display: "flex", gap: "0.25em" }}>
          <Form.Item
            {...field}
            noStyle
            // style={{
            //   width: "40%",
            // }}
            name={[name, 1, "text"]}
            rules={[{ required: true, message: "'text' is required" }]}
          >
            <TextArea placeholder="Text" />
          </Form.Item>
          <Form.Item
            {...field}
            noStyle
            // style={{
            //   width: "40%",
            // }}
            name={[name, 1, "tooltip"]}
          >
            <TextArea placeholder="Tooltip" />
          </Form.Item>
        </div>
      </div>
    )}
  </MultiEntry>
);
