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

import {
  DownCircleOutlined,
  MinusCircleOutlined,
  PlusOutlined,
  UpCircleOutlined,
} from "@ant-design/icons";
import { Button, Form, FormListFieldData } from "antd";
import { ReactNode } from "react";

export const MultiEntry = ({
  name,
  label,
  children,
}: {
  name: string | number | (string | number)[];
  label: string;
  children: (field: FormListFieldData) => ReactNode;
}) => (
  <Form.Item label={label}>
    <Form.List name={name}>
      {(fields, { add, remove, move }) => (
        <>
          {fields.map((field, i) => (
            <div
              key={field.key}
              style={{
                width: "100%",
                display: "flex",
                alignItems: "baseline",
                gap: "0.5em",
                marginBottom: "0.5em",
              }}
            >
              {children(field)}{" "}
              <div
                style={{
                  width: "20%",
                  display: "flex",
                  justifyContent: "flex-start",
                  gap: "0.25em",
                }}
              >
                <MinusCircleOutlined onClick={() => remove(field.name)} />
                {i > 0 && <UpCircleOutlined onClick={() => move(i, i - 1)} />}
                {i < fields.length - 1 && (
                  <DownCircleOutlined onClick={() => move(i, i + 1)} />
                )}
              </div>
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
