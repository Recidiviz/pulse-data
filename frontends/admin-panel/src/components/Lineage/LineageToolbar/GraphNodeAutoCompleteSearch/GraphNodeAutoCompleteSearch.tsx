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

import { useReactFlow } from "@xyflow/react";
import { AutoComplete, Input, message } from "antd";
import { observer } from "mobx-react-lite";
import { useState } from "react";

import { useUiStore } from "../../../../LineageStore/LineageRootContext";
import { getErrorMessage } from "../../../../LineageStore/Utils";
import { defaultFitViewOptions } from "../../Constants";

export const GraphNodeAutoCompleteSearch: React.FC = observer(() => {
  const { setZoomToBarExpanded, nodeZoomToSearchOptions } = useUiStore();

  const { fitView } = useReactFlow();
  const [searchValue, setSearchValue] = useState("");

  const handleSearch = (value: string) => {
    if (value.trim()) {
      try {
        fitView({
          nodes: [{ id: value.trim() }],
          ...defaultFitViewOptions,
        });
        setSearchValue("");
      } catch (e) {
        message.error(getErrorMessage(e));
        setSearchValue("");
      }
    }
  };

  return (
    <AutoComplete
      options={nodeZoomToSearchOptions}
      value={searchValue}
      filterOption
      onSelect={(value) => handleSearch(value)}
      onChange={(value) => setSearchValue(value)}
      defaultOpen
      autoFocus
      onDropdownVisibleChange={(open: boolean) => {
        if (open === false) {
          setZoomToBarExpanded(false);
        }
      }}
    >
      <Input
        className="node-search"
        placeholder="Search by view name..."
        allowClear
        onChange={(s) => setSearchValue(s.target.value)}
        size="large"
        style={{ width: "25vw" }}
      />
    </AutoComplete>
  );
});
