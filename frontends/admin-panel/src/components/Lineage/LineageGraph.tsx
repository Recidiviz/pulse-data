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

import {
  Background,
  Controls,
  MiniMap,
  Node,
  ReactFlow,
  useReactFlow,
} from "@xyflow/react";
import { message } from "antd";
import { observer } from "mobx-react-lite";
import { useCallback, useEffect } from "react";
import { useHistory, useLocation } from "react-router-dom";

import { useLineageRootStore } from "../../LineageStore/LineageRootContext";
import { getErrorMessage, isHydrated } from "../../LineageStore/Utils";
import { LINEAGE_BASE } from "../../navigation/Lineage";
import { defaultFitViewOptions } from "./Constants";
import BigQueryGraphNode from "./GraphNode/GraphNode";
import { GraphNodeDetailDrawer } from "./GraphNode/GraphNodeDetailDrawer/GraphNodeDetailDrawer";
import { LineageToolbar } from "./LineageToolbar/LineageToolbar";
import { NodeFilterModal } from "./LineageToolbar/NodeFilterModal/NodeFilterModal";

const nodeTypes = {
  view: BigQueryGraphNode,
};

function LineageGraph() {
  const {
    graphStore: {
      nodes,
      edges,
      resetGraphToUrn,
      handleReactFlowNodesChange,
      handleReactFlowEdgesChange,
      changeNodeHighlight,
    },
    lineageStore: { hydrate, hydrationState },
    uiStore: { setNodeDetailDrawerUrn },
  } = useLineageRootStore();

  const { fitView } = useReactFlow();

  const history = useHistory();
  const location = useLocation();

  const getData = useCallback(async () => {
    if (hydrationState && hydrationState.status === "needs hydration") {
      await hydrate();
    }
  }, [hydrationState, hydrate]);

  useEffect(() => {
    getData();
  }, [getData, hydrationState]);

  useEffect(() => {
    if (isHydrated(hydrationState) && location.search) {
      const startingUrn = new URLSearchParams(location.search).get("start_urn");
      if (startingUrn !== null) {
        try {
          resetGraphToUrn(startingUrn);
          setNodeDetailDrawerUrn(startingUrn);
          fitView({
            nodes: [{ id: startingUrn }],
            ...defaultFitViewOptions,
          });
        } catch (e) {
          message.error(getErrorMessage(e));
          history.push(LINEAGE_BASE);
        }
      }
    }
    // we only want this to happen on initial page load after we are hydrated
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [hydrationState]);

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
      minZoom={0.01}
    >
      <LineageToolbar />
      <MiniMap
        nodeColor={(n) => {
          if (n.data.type === "view") return "#4c6290";
          if (n.data.type === "source") return "#d9a95f";
          return "#eee";
        }}
        zoomable
        pannable
      />
      <Controls />
      <NodeFilterModal />
      <GraphNodeDetailDrawer />
      <Background gap={12} size={1} className="background-canvas" />
    </ReactFlow>
  );
}

export default observer(LineageGraph);
