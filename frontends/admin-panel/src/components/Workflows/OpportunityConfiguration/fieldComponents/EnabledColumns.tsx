// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2026 Recidiviz, Inc.
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

import { Alert, Form, Input, Select } from "antd";
import TextArea from "antd/lib/input/TextArea";

import { MultiEntryChild } from "../../formUtils/MultiEntry";
import { handlebarsValidator } from "./CriteriaCopy";

// Mirrors the sorting function names supported by @tanstack/react-table, which the frontend uses to render columns.
const SORTING_FN_OPTIONS = [
  "alphanumeric",
  "alphanumericCaseSensitive",
  "text",
  "textCaseSensitive",
  "datetime",
  "basic",
] as const;

export const EnabledColumnsView: MultiEntryChild = ({ name }) => (
  <Form.Item noStyle shouldUpdate>
    {({ getFieldValue }) => {
      const { columnId, columnHeader, cellValue, sortingFn } =
        getFieldValue(["enabledColumns", name]) ?? {};
      return (
        <div className="ant-form-text">
          <div>
            <b>Column ID: </b>
            {columnId}
          </div>
          <div>
            <b>Header: </b>
            {columnHeader || <i>Default</i>}
          </div>
          <div>
            <b>Cell Value: </b>
            {cellValue || <i>Default</i>}
          </div>
          <div>
            <b>Sorting Function: </b>
            {sortingFn || (
              <i>
                Default (FE will use FE-defined column sortingFn or assume
                alphanumeric)
              </i>
            )}
          </div>
        </div>
      );
    }}
  </Form.Item>
);

export const EnabledColumnsEdit: MultiEntryChild = ({ name }) => (
  <div style={{ width: "100%", marginBottom: "0.25em" }}>
    <Form.Item
      style={{ marginBottom: 0, width: "100%" }}
      name={[name, "columnId"]}
      rules={[
        { required: true, whitespace: true, message: "'columnId' is required" },
      ]}
    >
      <Input placeholder="Column ID" />
    </Form.Item>
    <div style={{ marginTop: "0.25em", display: "flex", gap: "0.25em" }}>
      <Form.Item
        noStyle
        name={[name, "columnHeader"]}
        normalize={(value) => value || null}
      >
        <Input placeholder="Header" />
      </Form.Item>
    </div>
    <div style={{ marginTop: "0.25em", display: "flex", gap: "0.25em" }}>
      <Form.Item
        style={{ marginBottom: 0, width: "100%" }}
        name={[name, "cellValue"]}
        normalize={(value) => value || null}
        rules={[{ validator: handlebarsValidator }]}
      >
        <TextArea placeholder="Cell value (accepts handlebars templates)" />
      </Form.Item>
    </div>
    <div
      style={{
        marginTop: "0.25em",
        display: "flex",
        flexDirection: "column",
        gap: "0.25em",
      }}
    >
      <Form.Item noStyle name={[name, "sortingFn"]}>
        <Select
          allowClear
          options={SORTING_FN_OPTIONS.map((value) => ({ label: value, value }))}
        />
      </Form.Item>
      <Form.Item noStyle shouldUpdate>
        {({ getFieldValue }) =>
          getFieldValue(["enabledColumns", name, "sortingFn"]) ===
            "datetime" && (
            <Alert
              type="info"
              showIcon
              message="For 'datetime' sorting to apply, you must set 'Cell value' and confirm the opportunity's FE schema output for this field is a Date."
            />
          )
        }
      </Form.Item>
    </div>
  </div>
);
