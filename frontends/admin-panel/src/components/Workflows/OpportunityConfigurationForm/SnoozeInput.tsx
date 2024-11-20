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

import { Form, InputNumber, Radio, RadioChangeEvent, Select } from "antd";

import { BabyOpportunityConfiguration } from "../../../WorkflowsStore/models/OpportunityConfiguration";

const WEEKDAYS = [
  "Sunday",
  "Monday",
  "Tuesday",
  "Wednesday",
  "Thursday",
  "Friday",
  "Saturday",
];

type SnoozeValue = BabyOpportunityConfiguration["snooze"];

type SnoozeInputProps = {
  value?: SnoozeValue;
  onChange?: (value: SnoozeValue) => void;
};

export const SnoozeInput: React.FC<SnoozeInputProps> = ({
  value,
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  onChange = () => {},
}) => {
  let snoozeType: "none" | "adjustable" | "automatic" = "none";
  if (value) {
    if ("defaultSnoozeDays" in value) snoozeType = "adjustable";
    else if ("autoSnoozeParams" in value) snoozeType = "automatic";
  }

  const changeSnoozeType = ({
    target: { value: newSnoozeType },
  }: RadioChangeEvent) => {
    if (newSnoozeType === "none") onChange(undefined);
    else if (newSnoozeType === "adjustable")
      onChange({ defaultSnoozeDays: 30, maxSnoozeDays: 90 });
    else if (newSnoozeType === "automatic")
      onChange({
        autoSnoozeParams: { type: "snoozeDays", params: { days: 30 } },
      });
  };

  return (
    <>
      <Form.Item>
        <Radio.Group
          value={snoozeType}
          onChange={changeSnoozeType}
          optionType="button"
        >
          <Radio value="none">None (Indefinite Denial)</Radio>
          <Radio value="adjustable">Adjustable</Radio>
          <Radio value="automatic">Automatic</Radio>
        </Radio.Group>
      </Form.Item>
      {value && "defaultSnoozeDays" in value && (
        <>
          <Form.Item
            label="Default Snooze Days"
            name={["snooze", "defaultSnoozeDays"]}
          >
            <InputNumber />
          </Form.Item>
          <Form.Item
            label="Maximum Snooze Days"
            name={["snooze", "maxSnoozeDays"]}
          >
            <InputNumber />
          </Form.Item>
        </>
      )}
      {value && "autoSnoozeParams" in value && (
        <>
          <Form.Item label="Type" name={["snooze", "autoSnoozeParams", "type"]}>
            <Select
              onChange={(t) => {
                if (t === "snoozeDays")
                  onChange({
                    autoSnoozeParams: {
                      type: "snoozeDays",
                      params: { days: 30 },
                    },
                  });
                else if (t === "snoozeUntil")
                  onChange({
                    autoSnoozeParams: {
                      type: "snoozeUntil",
                      params: { weekday: "Monday" },
                    },
                  });
              }}
            >
              <Select.Option value="snoozeDays">
                Snooze for a number of days
              </Select.Option>
              <Select.Option value="snoozeUntil">
                Snooze until a day of the week
              </Select.Option>
            </Select>
          </Form.Item>
          {value.autoSnoozeParams.type === "snoozeDays" && (
            <Form.Item
              label="Days"
              name={["snooze", "autoSnoozeParams", "params", "days"]}
            >
              <InputNumber />
            </Form.Item>
          )}
          {value.autoSnoozeParams.type === "snoozeUntil" && (
            <Form.Item
              label="Weekday"
              name={["snooze", "autoSnoozeParams", "params", "weekday"]}
            >
              <Select options={WEEKDAYS.map((d) => ({ value: d, label: d }))} />
            </Form.Item>
          )}
        </>
      )}
    </>
  );
};
