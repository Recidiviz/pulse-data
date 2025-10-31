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
import { buildNodeFilter } from "../../../../LineageStore/NodeFilter/Utils";
import { NodeFilterKey, NodeFilterType } from "../../../../LineageStore/types";
import { buildSelectOption } from "../../../../LineageStore/Utils";

type NodeFilterSelectProps = {
  mode: "multiple" | "tags";
  title: string;
  placeholder: string;
  options: BaseOptionType[];
  type: NodeFilterType;
  filterKey: NodeFilterKey;
  addCandidateFilter(filter: NodeFilter): void;
  removeCandidateFilter(
    key: NodeFilterKey,
    type?: NodeFilterType,
    value?: string
  ): void;
  initialFilters: NodeFilter[];
};
export const NodeFilterSelect: React.FC<NodeFilterSelectProps> = observer(
  ({
    mode,
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
          mode={mode}
          allowClear
          style={{ width: "100%" }}
          placeholder={placeholder}
          // typing is wrong here for value -- even though it claims to be an BaseOptionType
          // it is not -- it's always a string
          // eslint-disable-next-line @typescript-eslint/ban-ts-comment
          // @ts-ignore
          onSelect={(value: string) => {
            addCandidateFilter(buildNodeFilter(type, filterKey, value));
          }}
          onClear={() => removeCandidateFilter(filterKey)}
          // typing is wrong here for value -- even though it claims to be an BaseOptionType
          // it is not -- it's always a string
          // eslint-disable-next-line @typescript-eslint/ban-ts-comment
          // @ts-ignore
          onDeselect={(value: string) => {
            removeCandidateFilter(filterKey, type, value);
          }}
          defaultValue={initialFilters
            .filter((f) => f.type === type && f.key === filterKey)
            .map((f) => buildSelectOption(f.value))}
          options={options}
        />
      </>
    );
  }
);
