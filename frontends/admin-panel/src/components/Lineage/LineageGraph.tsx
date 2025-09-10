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

import "@xyflow/react/dist/style.css";
import "./LineageGraph.css";

import { Background, Controls, MiniMap, Node, ReactFlow } from "@xyflow/react";
import { observer } from "mobx-react-lite";
import { useCallback, useEffect } from "react";
import { useParams } from "react-router-dom";

import { useLineageRootStore } from "../../LineageStore/LineageRootContext";
import BigQueryGraphNode from "./GraphNode/GraphNode";

const nodeTypes = {
  view: BigQueryGraphNode,
};

function LineageGraph() {
  const {
    graphStore: {
      nodes,
      edges,
      selectedNode,
      resetGraphToActiveNode,
      handleReactFlowNodesChange,
      handleReactFlowEdgesChange,
      changeNodeHighlight,
    },
    lineageStore: { hydrate, hydrationState },
  } = useLineageRootStore();

  const { datasetId, viewId } = useParams<{
    datasetId?: string;
    viewId?: string;
  }>();

  const getData = useCallback(async () => {
    if (hydrationState && hydrationState.status === "needs hydration") {
      await hydrate();
    }
  }, [hydrationState, hydrate]);

  useEffect(() => {
    getData();
  }, [getData, hydrationState]);

  useEffect(() => {
    if (
      datasetId &&
      viewId &&
      hydrationState &&
      hydrationState.status === "hydrated"
    ) {
      const newActiveNode = `${datasetId}.${viewId}`;
      if (newActiveNode !== selectedNode) {
        resetGraphToActiveNode(newActiveNode);
      }
    }
  }, [datasetId, viewId, selectedNode, resetGraphToActiveNode, hydrationState]);

  return (
    <ReactFlow
      nodes={nodes}
      edges={edges}
      nodeTypes={nodeTypes}
      onNodesChange={handleReactFlowNodesChange}
      onEdgesChange={handleReactFlowEdgesChange}
      onNodeMouseEnter={(event: React.MouseEvent, node: Node) =>
        changeNodeHighlight(node.id, true)
      }
      onNodeMouseLeave={(event: React.MouseEvent, node: Node) =>
        changeNodeHighlight(node.id, false)
      }
      fitView
      colorMode="system"
      proOptions={{ hideAttribution: true }}
      debug
    >
      <MiniMap
        nodeColor={(n) => {
          if (n.data.type === "view") return "#4c6290";
          if (n.data.type === "source") return "#6e8c93";
          return "#eee";
        }}
        maskColor="#00000088"
      />
      <Controls />
      <Background gap={12} size={1} className="background-canvas" />
    </ReactFlow>
  );
}

export default observer(LineageGraph);
