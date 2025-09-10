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
import { useState } from "react";

import { useGraphStore } from "../../../LineageStore/LineageRootContext";
import { GraphDirection } from "../../../LineageStore/types";
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
  // TODO(#46966): implement contraction for mvp by using this state
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const [showSearch, setShowSearch] = useState(false);

  const { expandGraph, contractGraph } = useGraphStore();

  const iconComponent = isExpanded ? (
    <MinusOutlined size={18} />
  ) : (
    <PlusOutlined size={18} />
  );

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

  return (
    <>
      {/* // TODO(#46345): implement contraction for mvp by removing !isExpanded */}
      {hasNeighbors && !isExpanded && (
        <div
          className={
            "graph-node-container " +
            `graph-node-container-${direction.toLowerCase()} `
          }
          title={`Show ${direction.toLowerCase()}`}
        >
          {direction === GraphDirection.DOWNSTREAM && (
            <>
              <div
                className={
                  "graph-node-expand-arrow " +
                  `graph-node-expand-arrow-${direction.toLowerCase()} `
                }
                {...buttonizeWithEvent(handleContractOrExpand)}
              >
                {iconComponent}
              </div>
              <div
                className={
                  "graph-node-expand-search " +
                  `graph-node-expand-search-${direction.toLowerCase()}`
                }
                /* // TODO(#46966): implement downstream search */
                {...buttonize(() => setShowSearch(true))}
                title={`Expand to specific ${direction.toLowerCase()}`}
              >
                <SearchOutlined size={18} color="#eee" />
              </div>
            </>
          )}
          {direction === GraphDirection.UPSTREAM && (
            <>
              <div
                className={
                  "graph-node-expand-search " +
                  `graph-node-expand-search-${direction.toLowerCase()}`
                }
                /* // TODO(#46966): implement downstream search */
                {...buttonize(() => setShowSearch(true))}
                title={`Expand to specific ${direction.toLowerCase()}`}
              >
                <SearchOutlined size={18} color="#eee" />
              </div>
              <div
                className={
                  "graph-node-expand-arrow " +
                  `graph-node-expand-arrow-${direction.toLowerCase()} `
                }
                {...buttonizeWithEvent(handleContractOrExpand)}
              >
                {iconComponent}
              </div>
            </>
          )}
        </div>
      )}
    </>
  );
};
