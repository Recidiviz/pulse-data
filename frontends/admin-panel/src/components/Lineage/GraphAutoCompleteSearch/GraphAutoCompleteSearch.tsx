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
import "./GraphAutoCompleteSearch.css";

import { Panel, useReactFlow } from "@xyflow/react";
import { AutoComplete, Input, message } from "antd";
import { observer } from "mobx-react-lite";
import { useState } from "react";
import { useHistory } from "react-router-dom";

import { useLineageRootStore } from "../../../LineageStore/LineageRootContext";
import { getErrorMessage } from "../../../LineageStore/Utils";
import { LINEAGE_BASE } from "../../../navigation/Lineage";
import { defaultFitViewOptions } from "../Constants";

export const GraphAutoCompleteSearchBar: React.FC = observer(() => {
  const {
    graphStore: { selectedNode, resetGraphToActiveNode, autoCompleteOptions },
  } = useLineageRootStore();
  const history = useHistory();
  const { fitView } = useReactFlow();
  const [searchValue, setSearchValue] = useState("");

  const handleSearch = (value: string) => {
    if (value.trim() && value.trim() !== selectedNode) {
      try {
        resetGraphToActiveNode(value.trim());
        fitView({
          nodes: [{ id: value.trim() }],
          ...defaultFitViewOptions,
        });
        setSearchValue("");
        const [newDatasetId, newViewId] = value.split(".", 2);
        history.push(`${LINEAGE_BASE}/${newDatasetId}/${newViewId}`);
      } catch (e) {
        message.error(getErrorMessage(e));
        setSearchValue("");
      }
    }
  };

  return (
    <Panel position="top-left">
      <AutoComplete
        options={autoCompleteOptions}
        value={searchValue}
        filterOption
        onSelect={(value) => {
          handleSearch(value);
        }}
        // TODO(#46345): dynamically setting the view width to expand when we are
        // searching as some of the results can be hard to read
      >
        <Input.Search
          className="node-search"
          placeholder="Search by view name..."
          enterButton
          allowClear
          onChange={(s) => setSearchValue(s.target.value)}
          onSearch={(e) => handleSearch(e)}
          size="large"
          style={{ width: "25vw" }}
        />
      </AutoComplete>
    </Panel>
  );
});
