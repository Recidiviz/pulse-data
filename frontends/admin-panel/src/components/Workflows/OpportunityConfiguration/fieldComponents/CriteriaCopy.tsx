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
import { RuleObject } from "antd/lib/form";
import TextArea from "antd/lib/input/TextArea";
import Handlebars from "handlebars";

import { MultiEntryChild } from "../../formUtils/MultiEntry";
import { StaticValue } from "../../formUtils/StaticValue";

// How many arguments does each helper take?
// Using helpers not in this list will cause a validation error
const helperArities = {
  lowerCase: 1,
  upperCase: 1,
  titleCase: 1,
  date: 1,
  daysPast: 1,
  daysUntil: 1,
  monthsUntil: 1,
  yearsMonthsUntil: 1,
  daysToYearsMonthsPast: 1,
  eq: 2,
  length: 1,
  monthsOrDaysRemainingFromToday: 1,

  usMiSegregationDisplayName: 1,
};

const helpers = Object.fromEntries(
  Object.entries(helperArities).map(([h, arity]) => [
    h,
    (...args: unknown[]) => {
      if (args.length !== arity + 1) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const info = args[args.length - 1] as any;
        const raw = info.data.root.value as string;
        const {
          loc: { start, end },
        } = info;
        let snippet = raw.split("\n")[start.line - 1];
        if (start.line === end.line) {
          snippet = snippet.slice(start.column, end.column);
        } else {
          snippet = snippet.slice(start.column);
        }
        throw new Error(
          `Helper "${h}" expects ${arity} argument${
            arity > 1 ? "s" : ""
          } at line ${start.line}, column ${start.column}: ${snippet}`
        );
      }
    },
  ])
);

const handlebarsValidator: RuleObject["validator"] = async (_, value) => {
  if (!value) return;
  const template = Handlebars.compile(value, { noEscape: true });
  template({ value }, { helpers });
};

export const CriteriaCopyView: MultiEntryChild = ({ name }) => (
  <>
    <Form.Item noStyle name={[name, "key"]}>
      <StaticValue />
    </Form.Item>
    :
    <Form.Item noStyle name={[name, "text"]}>
      <StaticValue />
    </Form.Item>
    <Form.Item noStyle name={[name, "tooltip"]}>
      <StaticValue />
    </Form.Item>
  </>
);

export const CriteriaCopyEdit: MultiEntryChild = ({ name }) => (
  <div style={{ width: "100%", marginBottom: "0.25em" }}>
    <Form.Item
      noStyle
      name={[name, "key"]}
      rules={[{ required: true, message: "'criteria' is required" }]}
    >
      <Input placeholder="Criteria" />
    </Form.Item>
    <div style={{ marginTop: "0.25em", display: "flex", gap: "0.25em" }}>
      <Form.Item
        noStyle
        name={[name, "text"]}
        rules={[
          { required: true, message: "'text' is required" },
          { validator: handlebarsValidator },
        ]}
      >
        <TextArea placeholder="Text" />
      </Form.Item>
      <Form.Item
        noStyle
        name={[name, "tooltip"]}
        rules={[{ validator: handlebarsValidator }]}
      >
        <TextArea placeholder="Tooltip" />
      </Form.Item>
    </div>
  </div>
);

export const KeylessCriteriaCopyView: MultiEntryChild = ({ name }) => (
  <>
    <Form.Item noStyle name={[name, "text"]}>
      <StaticValue />
    </Form.Item>
    <Form.Item noStyle name={[name, "tooltip"]}>
      <StaticValue />
    </Form.Item>
  </>
);

export const KeylessCriteriaCopyEdit: MultiEntryChild = ({ name }) => (
  <div style={{ width: "100%", marginBottom: "0.25em" }}>
    <div style={{ display: "flex", gap: "0.25em" }}>
      <Form.Item
        noStyle
        name={[name, "text"]}
        rules={[
          { required: true, message: "'text' is required" },
          { validator: handlebarsValidator },
        ]}
      >
        <TextArea placeholder="Text" />
      </Form.Item>
      <Form.Item
        noStyle
        name={[name, "tooltip"]}
        rules={[{ validator: handlebarsValidator }]}
      >
        <TextArea placeholder="Tooltip" />
      </Form.Item>
    </div>
  </div>
);
