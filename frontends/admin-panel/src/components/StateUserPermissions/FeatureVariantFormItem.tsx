// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2023 Recidiviz, Inc.
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
import {
  Button,
  DatePicker,
  Form,
  FormInstance,
  Input,
  Select,
  Space,
} from "antd";
import { rem } from "polished";
import { useEffect, useState } from "react";
import styled from "styled-components/macro";

const SelectContainer = styled.div`
  .ant-picker {
    width: 145px;
    height: 32px;
  }

  > :first-child {
    padding-top: ${rem(12)};
  }
`;

export const FeatureVariantFormItem = ({
  disabled,
  form,
}: {
  disabled: boolean;
  form: FormInstance;
}): JSX.Element => {
  const [datePickerDisabled, setDatePickerDisabled] = useState(true);

  const options = [
    {
      value: true,
      label: "True",
    },
    {
      value: false,
      label: "False",
    },
  ];

  useEffect(() => {
    setDatePickerDisabled(true);
  }, [disabled]);

  return (
    <SelectContainer>
      <Form.List name="featureVariant">
        {(fields, { add, remove }) => {
          return (
            <>
              {fields.map(({ key, name, ...restField }) => (
                <Space key={key} align="baseline">
                  <Form.Item {...restField} name={[name, "name"]}>
                    <Input
                      disabled={disabled}
                      placeholder="Feature variant name"
                    />
                  </Form.Item>
                  <Form.Item
                    {...restField}
                    name={[name, "enabled"]}
                    labelCol={{ span: 13 }}
                  >
                    <Select
                      allowClear
                      style={{
                        width: 200,
                      }}
                      options={options}
                      onChange={(value) => setDatePickerDisabled(!value)}
                      disabled={disabled}
                    />
                  </Form.Item>
                  <Form.Item
                    {...restField}
                    name={[name, "activeDate"]}
                    labelCol={{ span: 13 }}
                  >
                    <DatePicker
                      showTime={{
                        format: "h:mm A",
                        minuteStep: 15,
                      }}
                      placeholder="Set Active Date"
                      disabled={datePickerDisabled || disabled}
                      allowClear
                    />
                  </Form.Item>
                  <MinusCircleOutlined onClick={() => remove(name)} />
                </Space>
              ))}
              <Form.Item>
                <Button
                  onClick={add}
                  icon={<PlusOutlined />}
                  disabled={disabled}
                >
                  Add a feature variant
                </Button>
              </Form.Item>
            </>
          );
        }}
      </Form.List>
    </SelectContainer>
  );
};

export default FeatureVariantFormItem;
