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

import {
  FilterOutlined,
  FilterTwoTone,
  LoginOutlined,
  PlusCircleOutlined,
  SwapOutlined,
} from "@ant-design/icons";
import { Panel } from "@xyflow/react";
import { Button, Tooltip } from "antd";
import { observer } from "mobx-react-lite";
import { useRef } from "react";

import { useLineageRootStore } from "../../../LineageStore/LineageRootContext";
import { GraphNodeAutoCompleteSearch } from "./GraphNodeAutoCompleteSearch/GraphNodeAutoCompleteSearch";
import { LineageGraphAutoCompleteSearch } from "./LineageGraphAutoCompleteSearch/LineageGraphAutoCompleteSearch";

export const LineageToolbar: React.FC = observer(() => {
  const {
    uiStore: {
      areFiltersActive,
      setFilterModalState,
      setSearchNewNodeExpanded,
      searchNewNodeExpanded,
      zoomToBarExpanded,
      setZoomToBarExpanded,
    },
    graphStore: { hasNodesInGraph },
  } = useLineageRootStore();

  const buttonRef = useRef<HTMLElement>(null);

  const searchComponentIcon = hasNodesInGraph ? (
    <SwapOutlined />
  ) : (
    <PlusCircleOutlined />
  );
  const searchComponentTooltip = hasNodesInGraph
    ? "Click to search for a node to replace the current graph with"
    : "Click to search for a node to add to the graph";

  const searchComponent = searchNewNodeExpanded ? (
    <LineageGraphAutoCompleteSearch />
  ) : (
    <Tooltip title={searchComponentTooltip}>
      <Button
        className="lineage-filter-button"
        size="large"
        icon={searchComponentIcon}
        onClick={() => {
          setSearchNewNodeExpanded(true);
        }}
      />
    </Tooltip>
  );

  const zoomToComponentTooltip = hasNodesInGraph
    ? "Click to search for a node displayed on the graph and zoom to it"
    : "Add nodes on the graph to zoom to one";

  const zoomToComponent = zoomToBarExpanded ? (
    <GraphNodeAutoCompleteSearch />
  ) : (
    <Tooltip title={zoomToComponentTooltip}>
      <Button
        className="lineage-filter-button"
        size="large"
        disabled={!hasNodesInGraph}
        icon={<LoginOutlined />}
        onClick={() => {
          setZoomToBarExpanded(true);
        }}
      />
    </Tooltip>
  );

  const filterButtonTooltipContent = hasNodesInGraph
    ? "Add or remove filters to adjust which nodes are displayed"
    : "Select node to apply filters";
  const filterComponent = (
    <Tooltip title={filterButtonTooltipContent}>
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
  );

  return (
    <Panel position="top-left">
      <div className="lineage-toolbar">
        <div className="lineage-search-bar-container">{searchComponent}</div>
        <div className="lineage-zoom-to-bar-container">{zoomToComponent}</div>
        <div className="lineage-filter-button-container">{filterComponent}</div>
      </div>
    </Panel>
  );
});
