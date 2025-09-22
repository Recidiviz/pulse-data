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
import "./GraphNode.css";

import { Handle, Node, NodeProps, Position } from "@xyflow/react";
import { AutoTextSize } from "auto-text-size";
import React, { memo } from "react";

import { useUiStore } from "../../../LineageStore/LineageRootContext";
import {
  BigQueryGraphDisplayNode,
  GraphDirection,
} from "../../../LineageStore/types";
import { buttonize } from "../../Utilities/GeneralUtilities";
import { BigQueryGraphNodeExpand } from "./GraphNodeExpandComponent";

const BigQueryGraphNode: React.FC<NodeProps<Node<BigQueryGraphDisplayNode>>> =
  memo(({ id, data }) => {
    const {
      viewId,
      datasetId,
      type,
      hasUpstream,
      hasDownstream,
      isExpandedUpstream,
      isExpandedDownstream,
    } = data;

    const { setNodeDetailDrawerUrn } = useUiStore();

    return (
      <div
        className={`graph-node-bbox graph-node-${type.toLowerCase()}`}
        {...buttonize(() => setNodeDetailDrawerUrn(id))}
      >
        <BigQueryGraphNodeExpand
          direction={GraphDirection.UPSTREAM}
          id={id}
          hasNeighbors={hasUpstream}
          isExpanded={isExpandedUpstream}
        />
        <div className="graph-node-header">
          <AutoTextSize
            minFontSizePx={1}
            maxFontSizePx={12}
            fontSizePrecisionPx={1}
            mode="box"
            className="graph-node-dataset-label"
          >
            {datasetId}
          </AutoTextSize>
        </div>

        <div className="graph-node-view-label">
          <AutoTextSize
            minFontSizePx={1}
            maxFontSizePx={20}
            fontSizePrecisionPx={1}
            mode="box"
          >
            {viewId}
          </AutoTextSize>
        </div>
        <BigQueryGraphNodeExpand
          direction={GraphDirection.DOWNSTREAM}
          id={id}
          hasNeighbors={hasDownstream}
          isExpanded={isExpandedDownstream}
        />

        <Handle
          type="target"
          position={Position.Left}
          isConnectable={false}
          style={{ opacity: 0 }}
        />
        <Handle
          type="source"
          position={Position.Right}
          isConnectable={false}
          style={{ opacity: 0 }}
        />
      </div>
    );
  });

export default BigQueryGraphNode;
