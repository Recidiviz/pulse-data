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

import { MinusOutlined, PlusOutlined, SearchOutlined } from "@ant-design/icons";

import {
  useGraphStore,
  useUiStore,
} from "../../../LineageStore/LineageRootContext";
import {
  GraphDirection,
  NodeDetailDrawerTab,
} from "../../../LineageStore/types";
import {
  buttonize,
  buttonizeWithEvent,
} from "../../Utilities/GeneralUtilities";

export const BigQueryGraphNodeExpand = ({
  id,
  direction,
  hasNeighbors,
  isExpanded,
}: {
  id: string;
  direction: GraphDirection;
  hasNeighbors?: boolean;
  isExpanded?: boolean;
}): JSX.Element => {
  const { expandGraph, contractGraph } = useGraphStore();
  const { setNodeDetailDrawerUrn, setActiveNodeDetailDrawerTab } = useUiStore();

  const iconComponent = isExpanded ? (
    <MinusOutlined size={18} />
  ) : (
    <PlusOutlined size={18} />
  );

  const handleAncestorSearch = () => {
    const tabToOpen =
      direction === GraphDirection.UPSTREAM
        ? NodeDetailDrawerTab.UPSTREAM_SEARCH
        : NodeDetailDrawerTab.DOWNSTREAM_SEARCH;
    setActiveNodeDetailDrawerTab(tabToOpen);
    setNodeDetailDrawerUrn(id);
  };

  const handleContractOrExpand = (
    e: React.MouseEvent | React.KeyboardEvent
  ) => {
    e.stopPropagation();
    if (isExpanded === true) {
      contractGraph(id, direction);
    } else {
      expandGraph(id, direction);
    }
  };

  const expandOrContractComponent = (
    <div
      className={
        "graph-node-expand-arrow " +
        `graph-node-expand-arrow-${direction.toLowerCase()} `
      }
      {...buttonizeWithEvent(handleContractOrExpand)}
    >
      {iconComponent}
    </div>
  );

  const searchComponent = (
    <div
      className={
        "graph-node-expand-search " +
        `graph-node-expand-search-${direction.toLowerCase()}`
      }
      {...buttonize(() => handleAncestorSearch())}
      title={`Expand to specific ${direction.toLowerCase()}`}
    >
      <SearchOutlined size={18} color="#eee" />
    </div>
  );

  return (
    <>
      {hasNeighbors && (
        <div
          className={
            "graph-node-container " +
            `graph-node-container-${direction.toLowerCase()} `
          }
          title={`Show ${direction.toLowerCase()}`}
        >
          {direction === GraphDirection.DOWNSTREAM && (
            <>
              {expandOrContractComponent}
              {searchComponent}
            </>
          )}
          {direction === GraphDirection.UPSTREAM && (
            <>
              {searchComponent}
              {expandOrContractComponent}
            </>
          )}
        </div>
      )}
    </>
  );
};
