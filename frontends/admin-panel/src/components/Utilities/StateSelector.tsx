// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2021 Recidiviz, Inc.
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
import { StateCodeInfo } from "../IngestOperationsView/constants";

interface StateSelectorProps {
  /* form component onChange that changes the stateCode */
  onChange?: (stateCode: string) => void;
  /* initial state code */
  initialValue?: string | null;
  value?: string;
  loading: boolean;
  /* array of available states */
  data: StateCodeInfo[] | undefined;
}

const StateSelector: React.FC<StateSelectorProps> = ({
  onChange,
  initialValue,
  loading,
  data,
  value,
}) => {
  const handleOnChange = (selectedValue: string) => {
    if (onChange) {
      onChange(selectedValue);
    }
  };

  const defaultValue = initialValue == null ? undefined : initialValue;

  return (
    <Select
      style={{ width: 200 }}
      placeholder="Select a state"
      loading={loading}
      optionFilterProp="children"
      defaultValue={defaultValue}
      onChange={handleOnChange}
      value={value}
      filterOption={(input, option) =>
        option?.props.children.toLowerCase().indexOf(input.toLowerCase()) >=
          0 ||
        option?.props.value.toLowerCase().indexOf(input.toLowerCase()) >= 0
      }
      filterSort={(optionA, optionB) =>
        optionA.children
          .toLowerCase()
          .localeCompare(optionB.children.toLowerCase())
      }
      showSearch
    >
      {data
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
