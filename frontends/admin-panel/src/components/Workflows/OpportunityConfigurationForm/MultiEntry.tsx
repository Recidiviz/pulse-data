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

import { MinusCircleOutlined, PlusOutlined } from "@ant-design/icons";
import { Button, Form, FormListFieldData } from "antd";
import { ReactNode } from "react";

export const MultiEntry = ({
  name,
  label,
  children,
}: {
  name: string;
  label: string;
  children: (field: FormListFieldData) => ReactNode;
}) => (
  <Form.Item label={label}>
    <Form.List name={name}>
      {(fields, { add, remove }) => (
        <>
          {fields.map((field) => (
            <div
              key={field.key}
              style={{
                width: "80%",
                display: "flex",
                alignItems: "baseline",
                gap: "0.5em",
                marginBottom: "0.5em",
              }}
            >
              {children(field)}{" "}
              <MinusCircleOutlined onClick={() => remove(field.name)} />
            </div>
          ))}
          <Form.Item>
            <Button type="dashed" onClick={() => add()} icon={<PlusOutlined />}>
              Add
            </Button>
          </Form.Item>
        </>
      )}
    </Form.List>
  </Form.Item>
);
