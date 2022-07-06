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
import { useFetchedDataJSON } from "../../hooks";
import { StateCodeInfo } from "../IngestOperationsView/constants";

interface StateSelectorProps {
  /** fetch function to initialize the list of states */
  fetchStateList: () => Promise<Response>;
  /** callback to receive the list of fetched states */
  onFetched?: (stateList: StateCodeInfo[]) => void;
  /** form component onChange that changes the stateInfo */
  onChange: (stateInfo: StateCodeInfo) => void;
  /** initial state code */
  initialValue?: string | null;
  value?: string;
}

const StateSelector: React.FC<StateSelectorProps> = ({
  fetchStateList,
  onFetched,
  onChange,
  initialValue,
  value,
}) => {
  const { loading, data } = useFetchedDataJSON<StateCodeInfo[]>(fetchStateList);

  // storing this on a ref to avoid infinite effect loops; it's only going to be called once
  const handleFetched = React.useRef(onFetched);
  React.useEffect(() => {
    if (handleFetched.current && data) {
      handleFetched.current(data);
    }
  }, [data]);

  const handleOnChange = (selectedValue: string) => {
    data?.forEach((state: StateCodeInfo) => {
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
      loading={loading}
      optionFilterProp="children"
      defaultValue={defaultValue}
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
