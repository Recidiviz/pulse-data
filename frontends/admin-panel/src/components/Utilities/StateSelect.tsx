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
import { Select } from "antd";
import * as React from "react";

import { StateCodeInfo } from "../general/constants";

interface StateSelectProps {
  states?: StateCodeInfo[];
  onChange: (stateInfo: StateCodeInfo) => void;
  initialValue?: string | null;
  value?: string;
  disabled?: boolean;
}

/**
 * State select component.
 * Should be used when passing the state list as a prop vs the fetch function in StateSelector component
 */
const StateSelector: React.FC<StateSelectProps> = ({
  states,
  onChange,
  initialValue,
  value,
  disabled,
}) => {
  const handleOnChange = (selectedValue: string) => {
    states?.forEach((state: StateCodeInfo) => {
      if (selectedValue === state.code) {
        onChange(state);
      }
    });
  };

  const defaultValue = initialValue == null ? undefined : initialValue;

  return (
    <Select
      style={{ width: 200 }}
      placeholder="Select a state"
      optionFilterProp="children"
      loading={!states}
      defaultValue={defaultValue}
      disabled={disabled}
      onChange={handleOnChange}
      value={value}
      filterOption={(input, option) =>
        (option?.children as unknown as string)
          .toLowerCase()
          .indexOf(input.toLowerCase()) >= 0 ||
        (option?.value as unknown as string)
          .toLowerCase()
          .indexOf(input.toLowerCase()) >= 0
      }
      filterSort={(optionA, optionB) =>
        (optionA?.children as unknown as string)
          .toLowerCase()
          .localeCompare((optionB?.children as unknown as string).toLowerCase())
      }
      showSearch
    >
      {states
        ?.sort((a, b) => a.name.localeCompare(b.name))
        .map((state: StateCodeInfo) => {
          return (
            <Select.Option key={state.code} value={state.code}>
              {state.name}
            </Select.Option>
          );
        })}
    </Select>
  );
};

export default StateSelector;
