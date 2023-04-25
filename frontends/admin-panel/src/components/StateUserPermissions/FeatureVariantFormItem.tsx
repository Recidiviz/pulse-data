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
import {
  DatePicker,
  DatePickerProps,
  Form,
  FormInstance,
  Input,
  Select,
  Space,
} from "antd";
import moment, { Moment } from "moment";
import { useEffect, useState } from "react";
import styled from "styled-components/macro";

const DATETIME_FORMAT = "YYYY-MM-DDTHH:mm:ssZ";

const SelectContainer = styled.div`
  .ant-picker {
    width: 145px;
    height: 32px;
    margin-bottom: 24px;
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
  const [activeDate, setActiveDate] = useState<Moment>();

  const options = [
    {
      value: JSON.stringify({}),
      label: "True",
    },
    {
      value: false,
      label: "False",
    },
  ];

  const handleDateChange = (value: DatePickerProps["value"]) => {
    setActiveDate(moment(value));
    form.setFieldsValue({
      featureVariantValue: JSON.stringify({
        activeDate: `${moment(value).format(DATETIME_FORMAT)}`,
      }),
    });
  };

  useEffect(() => {
    setActiveDate(undefined);
  }, [datePickerDisabled]);

  useEffect(() => {
    setDatePickerDisabled(true);
  }, [disabled]);

  return (
    <SelectContainer>
      <Space>
        <Form.Item name="featureVariantName">
          <Input disabled={disabled} placeholder="Feature variant name" />
        </Form.Item>
        <Form.Item name="featureVariantValue" labelCol={{ span: 13 }}>
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
        <DatePicker
          showTime
          onChange={handleDateChange}
          placeholder="Set Active Date"
          disabled={datePickerDisabled || disabled}
          format={DATETIME_FORMAT}
          allowClear={false}
          value={activeDate}
        />
      </Space>
    </SelectContainer>
  );
};

export default FeatureVariantFormItem;
