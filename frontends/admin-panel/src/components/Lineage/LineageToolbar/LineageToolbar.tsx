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
import "./LineageToolbar.css";

import { FilterOutlined, FilterTwoTone } from "@ant-design/icons";
import { Panel } from "@xyflow/react";
import { Button, Tooltip } from "antd";
import { observer } from "mobx-react-lite";
import { useRef } from "react";

import { useLineageRootStore } from "../../../LineageStore/LineageRootContext";
import { GraphAutoCompleteSearchBar } from "./GraphAutoCompleteSearch/GraphAutoCompleteSearch";

export const LineageToolbar: React.FC = observer(() => {
  const {
    uiStore: { areFiltersActive, setFilterModalState },
    graphStore: { hasNodesInGraph },
  } = useLineageRootStore();

  const buttonRef = useRef<HTMLElement>(null);
  const toolTipContent = hasNodesInGraph
    ? "Add or remove filters to adjust which nodes are displayed"
    : "Select node to apply filters";

  return (
    <Panel position="top-left">
      <div className="lineage-toolbar">
        <div className="lineage-search-bar-container">
          <GraphAutoCompleteSearchBar />
        </div>
        <div className="lineage-filter-button-container">
          <Tooltip title={toolTipContent}>
            <Button
              ref={buttonRef}
              disabled={!hasNodesInGraph}
              className={`
                lineage-filter-button
                lineage-filter-button-active-${areFiltersActive}`}
              size="large"
              icon={
                areFiltersActive ? (
                  <FilterTwoTone twoToneColor="#177ddc" />
                ) : (
                  <FilterOutlined />
                )
              }
              onClick={() => {
                setFilterModalState(true);
                if (buttonRef.current) {
                  buttonRef.current.blur();
                }
              }}
            />
          </Tooltip>
        </div>
      </div>
    </Panel>
  );
});
