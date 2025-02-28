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
import { Button, Form } from "antd";

export type MultiEntryChild = React.FC<{ name: number }>;

export const MultiEntry = ({
  name,
  label,
  readonly,
  child,
}: {
  name: string | number | (string | number)[];
  label: string;
  readonly?: boolean;
  child: MultiEntryChild;
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
              <Form.Item style={{ width: "100%" }}>{child(field)}</Form.Item>
              {!readonly && (
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
              )}
            </div>
          ))}
          {readonly && fields.length === 0 && <i>None</i>}
          {!readonly && (
            <Form.Item>
              <Button
                type="dashed"
                onClick={() => add()}
                icon={<PlusOutlined />}
              >
                Add
              </Button>
            </Form.Item>
          )}
        </>
      )}
    </Form.List>
  </Form.Item>
);
