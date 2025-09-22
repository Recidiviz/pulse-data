// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2025 Recidiviz, Inc.
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
import Select, { BaseOptionType } from "antd/lib/select";
import Title from "antd/lib/typography/Title";
import { observer } from "mobx-react-lite";

import { NodeFilter } from "../../../../LineageStore/NodeFilter/NodeFilter";
import { NodeFilterKey, NodeFilterType } from "../../../../LineageStore/types";
import { buildSelectOptionForFilter } from "../../../../LineageStore/Utils";

type NodeFilterSelectProps = {
  title: string;
  placeholder: string;
  options: BaseOptionType[];
  type: NodeFilterType;
  filterKey: NodeFilterKey;
  addCandidateFilter(filter: NodeFilter): void;
  removeCandidateFilter(type: NodeFilterType, filter?: NodeFilter): void;
  initialFilters: NodeFilter[];
};
export const NodeFilterSelect: React.FC<NodeFilterSelectProps> = observer(
  ({
    title,
    placeholder,
    options,
    type,
    filterKey,
    addCandidateFilter,
    removeCandidateFilter,
    initialFilters,
  }) => {
    return (
      <>
        <Title level={5}> {title} Filter </Title>
        <Select
          mode="multiple"
          allowClear
          style={{ width: "100%" }}
          placeholder={placeholder}
          onSelect={(value: BaseOptionType, option: BaseOptionType) => {
            addCandidateFilter(option.filter);
          }}
          onClear={() => removeCandidateFilter(type)}
          onDeselect={(value: BaseOptionType, option: BaseOptionType) => {
            removeCandidateFilter(type, option.filter);
          }}
          defaultValue={initialFilters
            .filter((f) => f.type === type && f.key === filterKey)
            .map(buildSelectOptionForFilter)}
          options={options}
        />
      </>
    );
  }
);
