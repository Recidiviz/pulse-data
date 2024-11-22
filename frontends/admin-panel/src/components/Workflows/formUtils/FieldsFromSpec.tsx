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

import { Checkbox, Divider, Form, Input } from "antd";
import { startCase } from "lodash";
import { Fragment } from "react/jsx-runtime";

import { MultiEntry, MultiEntryChild } from "./MultiEntry";
import { StaticValue } from "./StaticValue";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type FormComponent = React.FC<{ value?: any }>;

type FieldSpec = {
  label?: string;
  required?: boolean;
} & (
  | {
      multiple?: false;
      View?: FormComponent;
      Edit?: FormComponent;
    }
  | {
      multiple: true;
      View: MultiEntryChild;
      Edit: MultiEntryChild;
    }
);

export type FormSpec<T> = {
  sectionHeading: string;
  sectionSubhead?: string;
  fields: Partial<Record<keyof T, FieldSpec>>;
}[];

export function FieldsFromSpec<T>({
  spec,
  mode,
}: {
  spec: FormSpec<T>;
  mode: "view" | "edit";
}) {
  return (
    <>
      {spec.map(({ sectionHeading, sectionSubhead, fields }) => (
        <Fragment key={sectionHeading}>
          <Divider orientation="left">{sectionHeading}</Divider>
          {sectionSubhead && (
            <Form.Item>
              <i>{sectionSubhead}</i>
            </Form.Item>
          )}
          {Object.entries(fields).map(([name, fs]) => {
            const fieldSpec = fs as FieldSpec;
            const { label: customLabel, required, multiple } = fieldSpec;
            const label = customLabel ?? startCase(name);
            const isEdit = mode === "edit";
            if (multiple) {
              return (
                <MultiEntry
                  key={name}
                  name={name}
                  label={label}
                  child={isEdit ? fieldSpec.Edit : fieldSpec.View}
                  readonly={!isEdit}
                />
              );
            }
            const { View = StaticValue, Edit = Input } = fieldSpec;
            const Component = isEdit ? Edit : View;
            return (
              <Form.Item
                key={name}
                name={name}
                label={label}
                rules={required ? [{ required: true }] : []}
                valuePropName={
                  isEdit && Edit === Checkbox ? "checked" : undefined
                }
              >
                <Component />
              </Form.Item>
            );
          })}
        </Fragment>
      ))}
    </>
  );
}
